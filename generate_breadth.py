# generate_breadth_with_coingecko.py
# ───────────────────────────────────────────────────────────────────
import requests
import pandas as pd
import ta
import csv
import datetime as dt
import os
import sys
import time
import random

# ─────────── CONFIG ───────────
# 1) Use CoinGecko to get ALL USDT bases on Binance
# 2) Then for each base, build "BASEUSDT" as the trading symbol on Binance spots.
START = dt.datetime(2023, 1, 1)  # warm-up ≥200 days so EMA-200 clears by 2024-01-01

# Known stablebases to filter out (same as before)
STABLES = {
    'USDT','USDC','FDUSD','TUSD','DAI','USDP','BUSD','USDD',
    'AEUR','XUSD','USD1','PYUSD','PAXG','WBTC','WBETH'
}

# When fetching klines: rotate through these Binance hosts
KLINE_BASES = [
    "https://api-gcp.binance.com",
    "https://api1.binance.com",
    "https://api2.binance.com",
    "https://api3.binance.com"
]

# Browser-like header to minimize challenges
HDRS = {
    "User-Agent": "Mozilla/5.0 (GitHub Actions)",
    "Accept": "application/json"
}

# ─────────── HELPERS ───────────
def fetch_binance_usdt_bases(max_pages=50, pause=1.0):
    """
    Calls CoinGecko /exchanges/binance/tickers paginated,
    collects all unique base symbols where target == 'USDT'.
    """
    bases = set()
    for page in range(1, max_pages + 1):
        url = "https://api.coingecko.com/api/v3/exchanges/binance/tickers"
        params = {"per_page": 100, "page": page}
        r = requests.get(url, params=params, timeout=10)
        r.raise_for_status()
        data = r.json()

        tickers = data.get("tickers", [])
        if not tickers:
            break

        for t in tickers:
            if t.get("target") == "USDT":
                base = t.get("base")
                if base:
                    bases.add(base.upper())

        has_more = data.get("has_more", False)
        if not has_more:
            break
        time.sleep(pause)

    return sorted(bases)

def klines(sym, start_ms):
    """
    Rotate through KLINE_BASES until one returns valid JSON klines for 'sym' after 'start_ms'.
    Returns a DataFrame with columns ['time','close'] or exits if all fail.
    """
    for base in KLINE_BASES:
        try:
            out = []
            frm = start_ms
            while True:
                r = requests.get(
                    f"{base}/api/v3/klines",
                    params={"symbol": sym, "interval": "1d", "startTime": frm, "limit": 1000},
                    headers=HDRS,
                    timeout=15
                )
                ctype = r.headers.get("content-type", "")
                if not (r.ok and ctype.startswith("application/json")):
                    raise ValueError(f"bad response {r.status_code}")
                data = r.json()
                if not data:
                    break
                out += data
                frm = data[-1][0] + 86_400_000
                if len(data) < 1000:
                    break
            return pd.DataFrame({
                "time":  [int(k[0]) for k in out],
                "close": [float(k[4]) for k in out]
            })
        except Exception as e:
            print(f"⚠️  {sym}: {e} on {base} → trying next host…")
    sys.exit(f"❌  All kline hosts failed for {sym}")

def tv_write(col, out_fn, df):
    """
    Write a single‐series CSV appropriate for Pine Seeds:
      time (ms UTC), open, high, low, close (all = df[col]), volume=0
    """
    os.makedirs("data", exist_ok=True)
    with open(out_fn, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["time", "open", "high", "low", "close", "volume"])
        for t, v in zip(df["time"], df[col]):
            w.writerow([t, v, v, v, v, 0])

# ─────────── MAIN ───────────
print("⏳ Fetching USDT‐quoted bases from CoinGecko…")
bases = fetch_binance_usdt_bases()
print(f"  → found {len(bases)} bases on Binance that trade vs USDT.")

# Filter out known stablecoins (if you do not want to exclude stables, skip this)
bases = [b for b in bases if b not in STABLES]
print(f"  → after removing stable‐bases: {len(bases)} tokens remain.")

# Build full symbols ("BASEUSDT") for each coin
symbols = [f"{b}USDT" for b in bases]

print("⏳ Building breadth universe…")
rows = {}
start_ms = int(START.timestamp() * 1000)

for sym in symbols:
    try:
        df = klines(sym, start_ms)
    except SystemExit as e:
        print(f"⚠️  Skipping {sym}: {e}")
        continue

    if len(df) < 200:
        # skip coins that never warm up EMA-200
        print(f"⚠️  {sym} has only {len(df)} days; skipping.")
        continue

    df["ema75"]  = ta.trend.ema_indicator(df["close"], 75)
    df["ema200"] = ta.trend.ema_indicator(df["close"], 200)
    df["a75"]    = df["close"] > df["ema75"]
    df["a200"]   = df["close"] > df["ema200"]

    for t, a75, a200, e200 in zip(df["time"], df["a75"], df["a200"], df["ema200"]):
        if pd.isna(e200):
            continue
        if t not in rows:
            rows[t] = {"n": 0, "p75": 0, "p200": 0}
        rows[t]["n"]   += 1
        rows[t]["p75"] += int(a75)
        rows[t]["p200"]+= int(a200)

daily = pd.DataFrame({
    "time":    sorted(rows.keys()),
    "pct75":   [rows[t]["p75"]  / rows[t]["n"] * 100 for t in sorted(rows)],
    "pct200":  [rows[t]["p200"] / rows[t]["n"] * 100 for t in sorted(rows)]
})

tv_write("pct75",  "data/BR75.csv",  daily)
tv_write("pct200", "data/BR200.csv", daily)
print("✅ CSVs written to data/")
