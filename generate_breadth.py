# generate_breadth_coingecko.py
# ───────────────────────────────────────────────────────────────────────────
import requests
import pandas   as pd
import ta
import csv
import datetime as dt
import os
import sys
import time
import math

# ─────────── CONFIG ───────────
# 1) Use CoinGecko to get USDT bases on Binance
# 2) Use CoinGecko to get historical USD prices (daily) for each base
# Note: CoinGecko returns USD close ≈ USDT close for most large‐cap coins.

START    = dt.datetime(2023, 1, 1)   # ≥200 days before 2024-01-01 for EMA-200 warmup
PAUSE    = 1.2                       # seconds between CoinGecko calls to respect rate‐limit
PER_PAGE = 100                      # CoinGecko’s page size for /tickers

# stablebases to drop (if you want to exclude stable-coins)
STABLES = {
    'USDT','USDC','FDUSD','TUSD','DAI','USDP','BUSD','USDD',
    'AEUR','XUSD','USD1','PYUSD','PAXG','WBTC','WBETH'
}

# headers for CoinGecko
HDRS = {
    "User-Agent": "Mozilla/5.0 (GitHub Actions bot)",
    "Accept":      "application/json"
}

# ─────────── HELPER #1: Fetch USDT bases via CoinGecko ───────────
def fetch_binance_usdt_bases(max_pages=10):
    """
    1) Page through CoinGecko’s /exchanges/binance/tickers (up to max_pages),
    2) collect every `base` where `target=='USDT'`.
    Returns a sorted list of unique bases (e.g. ["ADA","AAVE","ATOM",...,"XRP"]).
    """
    bases = set()
    for page in range(1, max_pages + 1):
        url = "https://api.coingecko.com/api/v3/exchanges/binance/tickers"
        params = {"per_page": PER_PAGE, "page": page}
        r = requests.get(url, params=params, headers=HDRS, timeout=15)
        r.raise_for_status()
        data = r.json()
        tickers = data.get("tickers", [])
        if not tickers:
            break

        for t in tickers:
            if t.get("target") == "USDT":
                base = t.get("base", "").upper()
                if base:
                    bases.add(base)

        # CoinGecko sets "has_more" = False when done
        if not data.get("has_more", False):
            break
        time.sleep(PAUSE)

    return sorted(bases)

# ─────────── HELPER #2: Map each Binance base → CoinGecko id ───────────
def fetch_coin_list():
    """
    Returns a list of all coins (with their 'id' and 'symbol') from CoinGecko.
    We will build a map: symbol.lower() → id
    """
    url = "https://api.coingecko.com/api/v3/coins/list"
    r = requests.get(url, headers=HDRS, timeout=15)
    r.raise_for_status()
    return r.json()

def build_symbol_to_id_map(coin_list):
    """
    Given the list of {id, symbol, name} dicts, returns a dict:
      { symbol_upper: id_string }
    We uppercase symbols to match Binance bases.
    If multiple CG coins share the same symbol, pick the one with largest market cap rank?
    For simplicity, we pick the first occurrence.
    """
    mapping = {}
    for coin in coin_list:
        sym = coin.get("symbol","").upper()
        cid = coin.get("id")
        if sym and cid and sym not in mapping:
            mapping[sym] = cid
    return mapping

# ─────────── HELPER #3: Fetch historical daily USD closes via /market_chart/range ───────────
def fetch_historical_prices_usd(coin_id, start_ts, end_ts):
    """
    Calls CoinGecko’s /coins/{id}/market_chart/range?vs_currency=usd&from={start_ts}&to={end_ts}.
    Returns a DataFrame with one row per day (timestamp at midnight UTC) and column "close".
    CG returns hourly or 5-minute bars for large ranges—but the final points include 
    one data point per day at 00:00. 
    We’ll resample or dedupe to get exactly daily closes.
    """
    url = f"https://api.coingecko.com/api/v3/coins/{coin_id}/market_chart/range"
    params = {
        "vs_currency": "usd",
        "from": int(start_ts),
        "to":   int(end_ts)
    }
    r = requests.get(url, params=params, headers=HDRS, timeout=30)
    r.raise_for_status()
    data = r.json()

    # data["prices"] = [[timestamp_ms, price], ...]
    prices = data.get("prices", [])
    if not prices:
        return pd.DataFrame(columns=["time","close"])

    # Convert to DataFrame
    df = pd.DataFrame(prices, columns=["time","close"])
    df["time"] = df["time"].astype(int)  # still in ms
    # Convert ms -> Timestamp (UTC), then floor to midnight to group by date
    df["dt"] = pd.to_datetime(df["time"], unit="ms", utc=True).dt.floor("D")
    # For each dt, keep the last (i.e. we assume the last price of that day is the daily close)
    daily = df.groupby("dt", as_index=False).agg({"time":"last", "close":"last"})
    # Keep only those days ≥ START
    return daily[["time","close"]]

# ─────────── MAIN LOGIC ───────────
if __name__ == "__main__":
    # 1) Step 1: find all USDT bases on Binance
    print("⏳ Fetching USDT‐quoted bases from CoinGecko…")
    bases = fetch_binance_usdt_bases()
    print(f"  → found {len(bases)} bases on Binance vs USDT")

    # 2) Remove known stable‐bases (if desired)
    bases = [b for b in bases if b not in STABLES]
    print(f"  → after removing stable bases: {len(bases)} remain")
    if not bases:
        sys.exit("❌ No bases remain after filtering; aborting.")

    # 3) Build CoinGecko symbol→id map (once)
    print("⏳ Fetching CoinGecko coin list for id mapping…")
    coin_list = fetch_coin_list()
    sym2id = build_symbol_to_id_map(coin_list)

    # 4) Prepare date range in UNIX seconds
    start_ts = int(START.replace(tzinfo=dt.timezone.utc).timestamp())
    end_ts   = int(dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc).timestamp())

    # We'll accumulate a dict: { timestamp_ms: { n: count_total, p75: count_above75, p200: count_above200 } }
    rows = {}

    # 5) Loop over each base, fetch historical USD closes, compute EMAs, and tally breadth
    for base in bases:
        coin_id = sym2id.get(base)
        if not coin_id:
            print(f"⚠️  {base} not found in CoinGecko id map; skipping.")
            continue

        sym = f"{base}USDT"  # For logging only
        print(f"⏳  Fetching history for {sym} (CG id={coin_id})…")

        try:
            hist = fetch_historical_prices_usd(coin_id, start_ts, end_ts)
        except Exception as e:
            print(f"⚠️  {sym}: error fetching prices: {e}; skipping.")
            time.sleep(PAUSE)
            continue

        if hist.empty or len(hist) < 200:
            print(f"⚠️  {sym}: only {len(hist)} days of data (<200), skipping.")
            continue

        # Compute EMA-75 and EMA-200 on the "close" column
        hist["ema75"]  = ta.trend.ema_indicator(hist["close"], 75)
        hist["ema200"] = ta.trend.ema_indicator(hist["close"], 200)
        # Flags: close > ema
        hist["a75"]    = (hist["close"] > hist["ema75"]).astype(int)
        hist["a200"]   = (hist["close"] > hist["ema200"]).astype(int)

        # Tally counts for every day where ema200 is not NaN
        for tm, a75, a200, e200 in zip(
                hist["time"], hist["a75"], hist["a200"], hist["ema200"]):
            if pd.isna(e200):
                continue
            if tm not in rows:
                rows[tm] = {"n":0, "p75":0, "p200":0}
            rows[tm]["n"]   += 1
            rows[tm]["p75"] += int(a75)
            rows[tm]["p200"]+= int(a200)

        # Be kind: wait a bit to avoid CG rate-limit
        time.sleep(PAUSE)

    # 6) Build the final DataFrame
    if not rows:
        sys.exit("❌ No breadth data was tallied; aborting.")

    daily = pd.DataFrame({
        "time":    sorted(rows.keys()),
        "pct75":   [ rows[t]["p75"]  / rows[t]["n"] * 100 for t in sorted(rows) ],
        "pct200":  [ rows[t]["p200"] / rows[t]["n"] * 100 for t in sorted(rows) ]
    })

    # 7) Write CSVs for Pine-Seeds (exact same format as before)
    def tv_write(col, out_fn, df):
        os.makedirs("data", exist_ok=True)
        with open(out_fn, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["time","open","high","low","close","volume"])
            for t,v in zip(df["time"], df[col]):
                w.writerow([t,v,v,v,v,0])

    tv_write("pct75",  "data/BR75.csv",  daily)
    tv_write("pct200", "data/BR200.csv", daily)

    print(f"✅ Done – wrote {len(daily)} rows to data/BR75.csv and BR200.csv")
