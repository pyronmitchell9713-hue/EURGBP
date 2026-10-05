"""Download Dukascopy 1-minute BID candles into yearly UTC csv.gz files for the data layer.

    python -m tools.dukascopy_download USATECH --start 2012-01-01 --out data/raw
    python -m tools.dukascopy_download USA500 --start 2012-01-01 --end 2026-10-01

Writes {out}/{ROOT}_M1_BID_{year}_UTC.csv.gz with columns time_utc,open,high,low,close,volume
(bar open time in UTC). Flat filler candles (no volume, open == high == low == close) are dropped.
Meant to run where datafeed.dukascopy.com is reachable (e.g. the GitHub Action in
.github/workflows/dukascopy.yml); the Claude cloud session cannot reach it.

Dukascopy serves index CFDs, not CME futures: USATECH.IDX/USD tracks the Nasdaq-100 (NQ's index),
USA500.IDX/USD the S&P 500 (ES's index). There are no contracts or rolls. EURUSD, GBPUSD, AUDUSD and USDCAD are spot FX.
"""

from __future__ import annotations

import argparse
import concurrent.futures as cf
import lzma
import random
import time
import urllib.error
import urllib.request
from pathlib import Path

import numpy as np
import pandas as pd

INSTRUMENTS = {"USATECH": "USATECHIDXUSD", "USA500": "USA500IDXUSD", "EURUSD": "EURUSD", "GBPUSD": "GBPUSD", "AUDUSD": "AUDUSD", "USDCAD": "USDCAD"}
PLAUSIBLE = {"USATECH": (500, 60000), "USA500": (300, 15000), "EURUSD": (0.5, 3), "GBPUSD": (0.5, 3), "AUDUSD": (0.3, 2), "USDCAD": (0.8, 2)}  # close range
URL = "https://datafeed.dukascopy.com/datafeed/{inst}/{y}/{m:02d}/{d:02d}/BID_candles_min_1.bi5"
# seconds from day start, open, close, low, high (prices as scaled ints), volume
RECORD = np.dtype([("t", ">u4"), ("o", ">u4"), ("c", ">u4"), ("l", ">u4"), ("h", ">u4"), ("v", ">f4")])


HEADERS = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"}


def fetch_day(inst: str, day: pd.Timestamp, retries: int = 12) -> bytes | None:
    """Raw .bi5 bytes; b"" when Dukascopy has no file for that day; None if every retry failed."""
    url = URL.format(inst=inst, y=day.year, m=day.month - 1, d=day.day)  # Dukascopy months are 0-based
    last = ""
    for attempt in range(retries):
        try:
            with urllib.request.urlopen(urllib.request.Request(url, headers=HEADERS), timeout=60) as r:
                return r.read()
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return b""
            last = f"HTTP {e.code}"
        except (urllib.error.URLError, TimeoutError, ConnectionError) as e:
            last = repr(e)
        time.sleep(min(90, 2 ** attempt) * (0.5 + random.random()))  # the server answers 503 under load
    print(f"FAILED {url}: {last}", flush=True)
    return None


def parse_day(raw: bytes, day: pd.Timestamp) -> np.ndarray:
    if not raw:
        return np.empty((0, 6))
    rec = np.frombuffer(lzma.decompress(raw), dtype=RECORD)
    rows = np.column_stack([rec[k].astype(np.float64) for k in ("t", "o", "c", "l", "h", "v")])
    rows[:, 0] += day.value // 1_000_000_000  # epoch seconds
    return rows


def download(root: str, start: str, end: str, out: Path, workers: int = 3) -> list[Path]:
    inst = INSTRUMENTS[root]
    days = pd.date_range(start, end, freq="D")
    days = days[days.weekday != 5]  # Saturdays never trade
    t0 = time.time()
    with cf.ThreadPoolExecutor(workers) as ex:
        raws = list(ex.map(lambda d: fetch_day(inst, d), days))
    failed = [d for r, d in zip(raws, days) if r is None]
    if failed:  # one slower serial pass for the stragglers
        again = {d: fetch_day(inst, d, retries=8) for d in failed}
        raws = [again.get(d, r) if r is None else r for r, d in zip(raws, days)]
        failed = [d for r, d in zip(raws, days) if r is None]
        print(f"{len(failed)} days still missing after retry: {[str(d.date()) for d in failed[:20]]}", flush=True)
        if len(failed) > 0.01 * len(days):
            raise SystemExit("more than 1% of days failed to download")
    print(f"{root} {start}..{end}: fetched {len(days)} days in {time.time() - t0:.0f}s", flush=True)
    parts = [p for p in (parse_day(r or b"", d) for r, d in zip(raws, days)) if len(p)]
    if not parts:
        print(f"{root}: no data between {start} and {end}")
        return []
    arr = np.concatenate(parts)
    t, o, c, lo, hi, v = arr.T
    # prices are integers scaled by a power of ten; pick the scale that lands in a plausible range
    lo_ok, hi_ok = PLAUSIBLE[root]
    scale = next(s for s in (1000, 100, 10, 10000, 100000) if lo_ok <= np.median(c) / s <= hi_ok)
    df = pd.DataFrame({"time_utc": pd.to_datetime(t, unit="s", utc=True), "open": o / scale, "high": hi / scale,
                       "low": lo / scale, "close": c / scale, "volume": v})
    flat = (df["volume"] == 0) & (df["open"] == df["high"]) & (df["high"] == df["low"]) & (df["low"] == df["close"])
    df = df[~flat].drop_duplicates("time_utc").sort_values("time_utc")
    out.mkdir(parents=True, exist_ok=True)
    written = []
    for year, g in df.groupby(df["time_utc"].dt.year):
        path = out / f"{root}_M1_BID_{year}_UTC.csv.gz"
        g.assign(time_utc=g["time_utc"].dt.strftime("%Y-%m-%d %H:%M:%S")).to_csv(path, index=False)
        written.append(path)
        print(f"{path.name}: {len(g):,} bars, {g['time_utc'].min()} .. {g['time_utc'].max()}")
    print(f"{root}: scale 1/{scale}, {len(df):,} bars kept of {len(flat):,} downloaded")
    return written


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("root", choices=list(INSTRUMENTS))
    ap.add_argument("--start", default="2012-01-01")
    yesterday = pd.Timestamp.now("UTC").tz_localize(None).normalize() - pd.Timedelta(days=1)
    ap.add_argument("--end", default=yesterday.strftime("%Y-%m-%d"))
    ap.add_argument("--year", type=int, help="download one calendar year (overrides --start/--end)")
    ap.add_argument("--workers", type=int, default=3)
    ap.add_argument("--out", default="data/raw")
    a = ap.parse_args()
    if a.year:
        a.start = f"{a.year}-01-01"
        a.end = min(pd.Timestamp(f"{a.year}-12-31"), yesterday).strftime("%Y-%m-%d")
    download(a.root, a.start, a.end, Path(a.out), a.workers)
