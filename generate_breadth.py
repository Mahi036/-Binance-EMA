# generate_breadth_full_coingecko.py
# ─────────────────────────────────────────────────────────────────────────
import requests
import pandas   as pd
import ta
import csv
import datetime as dt
import os
import sys
import time

# ─────────── CONFIG ───────────
START    = dt.datetime(2023, 1, 1)  # ≥200 days before 2024-01-01 for EMA-200 warmup
PAUSE    = 3.0                     # ~20 calls/min → very safe vs CG rate limits
PER_PAGE = 100                     # tickers per page from CoinGecko

# Known stablecoin bases to exclude (you can adjust if needed)
STABLES = {
    'USDT','USDC','FDUSD','TUSD','DAI','USDP','BUSD','USDD',
    'AEUR','XUSD','USD1','PYUSD','PAXG','WBTC','WBETH'
}

# Standard “browser-like” header to reduce bot-blocks
HDRS = {
    "User-Agent": "Mozilla/5.0 (GitHub Actions bot)",
    "Accept":      "application/json"
}

# ─────────── HELPER 1: Fetch every USDT base on Binance via CoinGecko ───────────
def fetch_all_binance_usdt_bases(max_pages=50):
    """
    Pages through CoinGecko’s /exchanges/binance/tickers to collect
    every unique 'base' where target == 'USDT'. Returns a sorted list.
    """
    bases = set()
    for page in range(1, max_pages + 1):
        url = "https://api.coingecko.com/api/v3/exchanges/binance/tickers"
        params = {"per_page": PER_PAGE, "page": page}
        try:
            r = requests.get(url, params=params, headers=HDRS, timeout=15)
            r.raise_for_status()
            data = r.json()
        except Exception as e:
            print(f"⚠️  Failed to fetch tickers page {page}: {e}")
            break

        tickers = data.get("tickers", [])
        if not tickers:
            break

        for t in tickers:
            if t.get("target") == "USDT":
                base = t.get("base", "").upper()
                if base:
                    bases.add(base)

        # Stop if no more pages
        if not data.get("has_more", False):
            break

        time.sleep(PAUSE)

    return sorted(bases)

# ─────────── HELPER 2: Build map of symbol→CoinGecko ID ───────────
def fetch_coin_list():
    """
    Returns the full list of CoinGecko coins: each dict has 'id', 'symbol', 'name'.
    """
    url = "https://api.coingecko.com/api/v3/coins/list"
    r = requests.get(url, headers=HDRS, timeout=15)
    r.raise_for_status()
    return r.json()

def build_symbol_to_id_map(coin_list):
    """
    Returns dict { SYMBOL_UPPER: coin_id } for fast lookup.
    If multiple coins share the same symbol, the first is used.
    """
    m = {}
    for coin in coin_list:
        sym = coin.get("symbol", "").upper()
        cid = coin.get("id")
        if sym and cid and sym not in m:
            m[sym] = cid
    return m

# ─────────── HELPER 3: Fetch daily USD prices via CG `/market_chart/range` ───────────
def fetch_historical_prices_usd(coin_id, start_ts, end_ts):
    """
    Returns a DataFrame with columns ['time','close'] at 1 row per day
    (timestamp = ms at 00:00 UTC) by calling:
       /coins/{id}/market_chart/range?vs_currency=usd&from=…&to=…
    Implements up to 3 total attempts if 429 is encountered (backing off 5 sec).
    Raises:
      - PermissionError if 401 returned
      - RuntimeError if 429 persists after retries
      - Exception otherwise
    """
    url = f"https://api.coingecko.com/api/v3/coins/{coin_id}/market_chart/range"
    params = {"vs_currency": "usd", "from": start_ts, "to": end_ts}

    attempts = 0
    while attempts < 3:
        attempts += 1
        r = requests.get(url, params=params, headers=HDRS, timeout=30)
        status = r.status_code

        if status == 200 and r.headers.get("content-type","").startswith("application/json"):
            data = r.json().get("prices", [])
            if not data:
                return pd.DataFrame(columns=["time","close"])
            df = pd.DataFrame(data, columns=["time","close"])
            df["time"] = df["time"].astype(int)
            # floor to UTC midnight per record
            df["dt"] = pd.to_datetime(df["time"], unit="ms", utc=True).dt.floor("D")
            daily = df.groupby("dt", as_index=False).agg({"time":"last","close":"last"})
            return daily[["time","close"]]

        if status == 401:
            raise PermissionError(f"401 Unauthorized (no history for {coin_id})")

        if status == 429:
            print(f"⚠️  {coin_id}: 429 Rate Limit (attempt {attempts}); sleeping 5 sec…")
            time.sleep(5)
            continue

        # Other errors
        r.raise_for_status()

    # If we exhausted retries on 429
    raise RuntimeError(f"429 persisted after retries for {coin_id}")

# ─────────── MAIN ───────────
if __name__ == "__main__":
    # 1) Fetch the complete USDT base universe from CoinGecko
    print("⏳ Fetching ALL USDT‐quoted bases from Binance (via CoinGecko)…")
    all_bases = fetch_all_binance_usdt_bases()
    print(f"  → found {len(all_bases)} distinct USDT bases on Binance")

    # 2) Remove known stablecoin bases
    bases = [b for b in all_bases if b not in STABLES]
    print(f"  → after filtering stables: {len(bases)} tokens remain")

    if not bases:
        sys.exit("❌ No symbols left after filtering—aborting.")

    # 3) Build CoinGecko symbol→id map
    print("⏳ Fetching CoinGecko /coins/list for ID mapping…")
    coin_list = fetch_coin_list()
    sym2id = build_symbol_to_id_map(coin_list)

    # 4) Prepare timestamps (in Unix seconds) for the range query
    start_ts = int(START.replace(tzinfo=dt.timezone.utc).timestamp())
    end_ts   = int(dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc).timestamp())

    # 5) Loop over every base, fetch history, compute EMAs, tally breadth
    rows = {}  # { timestamp_ms: { 'n':count, 'p75':count_above75, 'p200':count_above200 } }

    for base in bases:
        coin_id = sym2id.get(base)
        if not coin_id:
            print(f"⚠️   {base} → no CoinGecko ID found; skipping.")
            time.sleep(PAUSE)
            continue

        symbol = f"{base}USDT"
        print(f"⏳   Fetching history for {symbol} (CG id={coin_id})…")
        try:
            hist = fetch_historical_prices_usd(coin_id, start_ts, end_ts)
        except PermissionError as pe:
            print(f"⚠️   {symbol}: {pe}; skipping.")
            time.sleep(PAUSE)
            continue
        except RuntimeError as re:
            print(f"⚠️   {symbol}: {re}; skipping.")
            time.sleep(PAUSE)
            continue
        except Exception as e:
            print(f"⚠️   {symbol}: unexpected error {e}; skipping.")
            time.sleep(PAUSE)
            continue

        if hist.empty or len(hist) < 200:
            print(f"⚠️   {symbol}: only {len(hist)} days (<200), skipping.")
            time.sleep(PAUSE)
            continue

        # Compute EMA-75 and EMA-200 on the daily 'close'
        hist["ema75"]  = ta.trend.ema_indicator(hist["close"], 75)
        hist["ema200"] = ta.trend.ema_indicator(hist["close"], 200)
        hist["a75"]    = (hist["close"] > hist["ema75"]).astype(int)
        hist["a200"]   = (hist["close"] > hist["ema200"]).astype(int)

        # Tally into rows[timestamp]
        for tm, a75, a200, e200 in zip(hist["time"], hist["a75"], hist["a200"], hist["ema200"]):
            if pd.isna(e200):
                continue
            if tm not in rows:
                rows[tm] = {"n":0, "p75":0, "p200":0}
            rows[tm]["n"]   += 1
            rows[tm]["p75"] += int(a75)
            rows[tm]["p200"]+= int(a200)

        time.sleep(PAUSE)

    # 6) Build the final breadth DataFrame
    if not rows:
        print("⚠️  No breadth data tallied for any symbol; writing only headers.")
        daily = pd.DataFrame(columns=["time","pct75","pct200"])
    else:
        daily = pd.DataFrame({
            "time":   sorted(rows.keys()),
            "pct75":  [ rows[t]["p75"]  / rows[t]["n"] * 100 for t in sorted(rows) ],
            "pct200": [ rows[t]["p200"] / rows[t]["n"] * 100 for t in sorted(rows) ]
        })
        print(f"✅ Built daily DataFrame with {len(daily)} rows")

    # 7) Write out CSVs for Pine-Seeds
    os.makedirs("data", exist_ok=True)
    with open("data/BR75.csv", "w", newline="") as f75, \
         open("data/BR200.csv","w", newline="") as f200:

        w75 = csv.writer(f75); w200 = csv.writer(f200)
        w75.writerow(["time","open","high","low","close","volume"])
        w200.writerow(["time","open","high","low","close","volume"])

        for t, pct75, pct200 in zip(daily["time"], daily.get("pct75", []), daily.get("pct200", [])):
            w75.writerow([t, pct75, pct75, pct75, pct75, 0])
            w200.writerow([t, pct200, pct200, pct200, pct200, 0])

    print("✅ CSVs written to data/BR75.csv + data/BR200.csv")
