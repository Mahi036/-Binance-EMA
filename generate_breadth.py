# generate_breadth.py
# ───────────────────────────────────────────────────────────────────
import pandas as pd
import requests
import ta
import csv
import datetime as dt
import os
import sys
import time
import random

# ─────────── CONFIG ───────────
# 1) “Raw” snapshot of exchangeInfo.json, maintained daily by Binance docs
#    (current path as of 2025-06; never blocked, always JSON)
# 2) Falling back to api-gcp (and api1/api2/api3/the main) if snapshot fails
EXCHANGEINFO_SOURCES = [
    # (1) read-only JSON file on GitHub
    "https://raw.githubusercontent.com/binance/binance-spot-api-docs/master/openapi/spot-api-json/exchangeInfo.json",
    # (2) public Binance endpoints (GCP host + backup domains)
    "https://api-gcp.binance.com/api/v3/exchangeInfo",
    "https://api1.binance.com/api/v3/exchangeInfo",
    "https://api2.binance.com/api/v3/exchangeInfo",
    "https://api3.binance.com/api/v3/exchangeInfo",
]

# When fetching klines, rotate through these if one is blocked
KLINE_BASES = [
    "https://api-gcp.binance.com",
    "https://api1.binance.com",
    "https://api2.binance.com",
    "https://api3.binance.com"
]

# Start ≥200 days before 2024-01-01 so EMA-200 warms up
START   = dt.datetime(2023, 1, 1)

STABLES = {
    'USDT','USDC','FDUSD','TUSD','DAI','USDP','BUSD','USDD',
    'AEUR','XUSD','USD1','PYUSD','PAXG','WBTC','WBETH'
}

# A normal “browser‐like” header to avoid Cloudflare challenges
HDRS = {
    "User-Agent": "Mozilla/5.0 (GitHub Actions)",
    "Accept": "application/json"
}

# ─────────── HELPERS ───────────
def get_json_with_retry(urls, max_try=5):
    """
    Try each URL in order. For each URL, attempt up to max_try times.
    If we get a 200 with JSON, return r.json().
    Otherwise, keep trying; on total failure, sys.exit().
    """
    for url in urls:
        for attempt in range(1, max_try + 1):
            try:
                r = requests.get(url, headers=HDRS, timeout=15)
                ctype = r.headers.get("content-type", "")
                # Accept only application/json (avoid HTML blocks)
                if r.ok and ctype.startswith("application/json"):
                    return r.json()
                print(f"⚠️  [{attempt}] non-JSON ({r.status_code}) from {url}")
            except requests.exceptions.RequestException as e:
                print(f"⚠️  [{attempt}] {e} from {url}")
            time.sleep(2 + random.random() * 2)
    sys.exit("❌  All exchangeInfo sources failed")

def universe():
    """
    Fetch the full exchangeInfo.json (or exit if unreachable),
    then return the list of all USDT‐quoted, non‐stable spot symbols.
    """
    data = get_json_with_retry(EXCHANGEINFO_SOURCES)
    return [
        s["symbol"]
        for s in data["symbols"]
        if s["status"] == "TRADING"
        and s["isSpotTradingAllowed"]
        and s["quoteAsset"] == "USDT"
        and s["baseAsset"] not in STABLES
    ]

def klines(sym, start_ms):
    """
    Rotate through KLINE_BASES until one returns valid JSON klines for 'sym' after 'start_ms'.
    Returns a DataFrame with columns ['time','close'].
    Exits if every host fails.
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
    Write a single‐series CSV suitable for Pine Seeds:
      time (ms UTC), open,high,low,close (all = df[col]), volume=0
    """
    os.makedirs("data", exist_ok=True)
    with open(out_fn, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["time", "open", "high", "low", "close", "volume"])
        for t, v in zip(df["time"], df[col]):
            w.writerow([t, v, v, v, v, 0])

# ─────────── MAIN ───────────
print("⏳ Building universe…")
syms = universe()
print("Universe size:", len(syms))

rows = {}
start_ms = int(START.timestamp() * 1000)

for sym in syms:
    df = klines(sym, start_ms)
    if len(df) < 200:
        continue    # skip symbols that never warm up EMA-200

    df["ema75"]  = ta.trend.ema_indicator(df["close"], 75)
    df["ema200"] = ta.trend.ema_indicator(df["close"], 200)
    df["a75"]    = df["close"] > df["ema75"]
    df["a200"]   = df["close"] > df["ema200"]

    for t, a75, a200, e200 in zip(df["time"], df["a75"], df["a200"], df["ema200"]):
        if pd.isna(e200):
            continue   # still warming up
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
