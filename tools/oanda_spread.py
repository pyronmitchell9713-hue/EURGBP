"""Typical OANDA spread per instrument, measured from 1-minute BID and ASK candle closes over the last N days.

Prints one line per root: median spread over all minutes and over the busy hours (13:00-17:00 UTC), in price units.
Nothing is written; read the result from the job log. Key from OANDA_API_KEY, as in oanda_download.py.

OANDA_API_KEY=... python -m tools.oanda_spread WTICO BCO --days 30
"""
import argparse, os, statistics, sys, urllib.parse
from datetime import datetime, timedelta, timezone

from tools.oanda_download import INSTRUMENTS, get, pick_host


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("roots", nargs="+", choices=sorted(INSTRUMENTS))
    ap.add_argument("--days", type=int, default=30)
    a = ap.parse_args()
    key = os.environ.get("OANDA_API_KEY", "").strip() or sys.exit("OANDA_API_KEY is not set")
    host = pick_host(key)
    for root in a.roots:
        frm, end, sp, busy = datetime.now(timezone.utc) - timedelta(days=a.days), datetime.now(timezone.utc), [], []
        while frm < end:
            q = {"price": "BA", "granularity": "M1", "count": 5000, "from": frm.strftime("%Y-%m-%dT%H:%M:%SZ")}
            cs = get(f"{host}/v3/instruments/{INSTRUMENTS[root]}/candles?" + urllib.parse.urlencode(q), key).get("candles", [])
            if not cs:
                break
            for c in cs:
                s = float(c["ask"]["c"]) - float(c["bid"]["c"])
                sp.append(s)
                if 13 <= int(c["time"][11:13]) < 17:
                    busy.append(s)
            last = datetime.fromisoformat(cs[-1]["time"][:19]).replace(tzinfo=timezone.utc)
            if last <= frm:
                break
            frm = last + timedelta(minutes=1)
        print(f"SPREAD {root} minutes={len(sp)} median={statistics.median(sp):.6g} busy_median={statistics.median(busy) if busy else float('nan'):.6g}"
              f" p90={sorted(sp)[int(len(sp) * .9)]:.6g}", flush=True)


if __name__ == "__main__":
    main()
