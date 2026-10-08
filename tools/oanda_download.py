"""1-minute BID candles from the OANDA v20 REST API for one instrument and one year.

Same file layout as the Dukascopy files (time_utc,open,high,low,close,volume), written to
<out>/<ROOT>_OANDA_M1_BID_<year>_UTC.csv.gz. The API key comes from the OANDA_API_KEY environment
variable (a GitHub secret); it is never written anywhere. Practice and live servers are both tried.

OANDA_API_KEY=... python -m tools.oanda_download EURUSD --year 2024 --out out
"""
import argparse, gzip, json, os, sys, time, urllib.error, urllib.parse, urllib.request
from datetime import datetime, timezone

INSTRUMENTS = {"EURUSD": "EUR_USD", "GBPUSD": "GBP_USD", "AUDUSD": "AUD_USD", "USDCAD": "USD_CAD"}
HOSTS = ("https://api-fxpractice.oanda.com", "https://api-fxtrade.oanda.com")
COUNT = 5000


def get(url, key, tries=6):
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {key}", "Accept-Datetime-Format": "RFC3339"})
    for i in range(tries):
        try:
            with urllib.request.urlopen(req, timeout=60) as r:
                return json.load(r)
        except urllib.error.HTTPError as e:
            if e.code in (401, 403) or i == tries - 1:
                raise
        except (urllib.error.URLError, TimeoutError):
            if i == tries - 1:
                raise
        time.sleep(2 ** i)


def pick_host(key):
    for h in HOSTS:
        try:
            get(f"{h}/v3/accounts", key, tries=2)
            return h
        except urllib.error.HTTPError as e:
            print(h, "->", e.code, file=sys.stderr)
    sys.exit("OANDA rejected the key on both practice and live servers (check OANDA_API_KEY)")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("root", choices=sorted(INSTRUMENTS))
    ap.add_argument("--year", type=int, required=True)
    ap.add_argument("--out", default="out")
    a = ap.parse_args()
    key = os.environ.get("OANDA_API_KEY", "").strip()
    if not key:
        sys.exit("OANDA_API_KEY is not set")
    host = pick_host(key)
    start = datetime(a.year, 1, 1, tzinfo=timezone.utc)
    end = min(datetime(a.year + 1, 1, 1, tzinfo=timezone.utc), datetime.now(timezone.utc))
    rows, frm, first = [], start.strftime("%Y-%m-%dT%H:%M:%SZ"), True
    while True:
        q = {"price": "B", "granularity": "M1", "count": COUNT, "from": frm, "includeFirst": str(first).lower()}
        data = get(f"{host}/v3/instruments/{INSTRUMENTS[a.root]}/candles?" + urllib.parse.urlencode(q), key)
        cs = data.get("candles", [])
        if not cs:
            break
        for c in cs:
            t = datetime.fromisoformat(c["time"][:19]).replace(tzinfo=timezone.utc)
            if t >= end:
                break
            if c.get("complete"):
                b = c["bid"]
                rows.append(f"{t:%Y-%m-%d %H:%M:%S},{b['o']},{b['h']},{b['l']},{b['c']},{c['volume']}")
        last = datetime.fromisoformat(cs[-1]["time"][:19]).replace(tzinfo=timezone.utc)
        if last >= end or len(cs) < COUNT:   # fewer than asked only when the newest candle is reached
            break
        frm, first = cs[-1]["time"], False
    os.makedirs(a.out, exist_ok=True)
    path = os.path.join(a.out, f"{a.root}_OANDA_M1_BID_{a.year}_UTC.csv.gz")
    with gzip.open(path, "wt") as f:
        f.write("time_utc,open,high,low,close,volume\n" + "\n".join(rows) + ("\n" if rows else ""))
    print(a.root, a.year, len(rows), "bars", rows[0][:19] if rows else "-", rows[-1][:19] if rows else "-")


if __name__ == "__main__":
    main()
