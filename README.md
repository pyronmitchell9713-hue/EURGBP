# EURGBP data

Dukascopy 1-minute BID candles for EURUSD and GBPUSD, 2020 onwards, in `data/raw/<PAIR>_M1_BID_<year>_UTC.csv.gz`
(columns time_utc, open, high, low, close, volume; UTC bar open time).

This repo only holds the data and the download job that fetches it (`.github/workflows/dukascopy.yml`, run by hand).
