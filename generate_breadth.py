# generate_breadth_full_coingecko.py  (PAUSE = 6.0, 1 retry on 429)
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
START    = dt.datetime(2023, 1, 1)   # ensure ≥200 days before 2024-01-01
PAUSE    = 6.0                       # ~10 calls/min → avoids 429 almost guaranteed
PER_PAGE = 100

STABLES = {
    'USDT','USDC','FDUSD','TUSD','DAI','USDP','BUSD','USDD',
    'AEUR','XUSD','USD1','PYUSD','PAXG','WBTC','WBETH'
}

HDRS = {
    "User-Agent": "Mozilla/5.0 (GitHub Actions bot)",
    "Accept":      "application/json"
}

# ─────────── HELPER 1: pull every USDT base via CoinGecko ───────────
def fetch_all_binance_usdt_bases(max_pages=50):
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

        if not data.get("has_more", False):
            break

        time.sleep(PAUSE)

    return sorted(bases)

# ─────────── HELPER 2: coin list → symbol→id map ───────────
def fetch_coin_list():
    url = "https://api.coingecko.com/api/v3/coins/list"
    r = requests.get(url, headers=HDRS, timeout=15)
    r.raise_for_status()
    return r.json()

def build_symbol_to_id_map(coin_list):
    m = {}
    for coin in coin_list:
        sym = coin.get("symbol", "").upper()
        cid = coin.get("id")
        if sym and cid and sym not in m:
            m[sym] = cid
    return m

# ─────────── HELPER 3: fetch daily USD close via /market_chart/range ───────────
def fetch_historical_prices_usd(coin_id, start_ts, end_ts):
    """
    One retry on 429 (rate limit). PAUSE between each coin is 6 s,
    so hitting 429 again means skipping.
    """
    url = f"https://api.coingecko.com/api/v3/coins/{coin_id}/market_chart/range"
    params = {"vs_currency": "usd", "from": start_ts, "to": end_ts}

    r = requests.get(url, params=params, headers=HDRS, timeout=30)
    status = r.status_code

    if status == 200 and r.headers.get("content-type","").startswith("application/json"):
        data = r.json().get("prices", [])
        if not data:
            return pd.DataFrame(columns=["time","close"])
        df = pd.DataFrame(data, columns=["time","close"])
        df["time"] = df["time"].astype(int)
        df["dt"] = pd.to_datetime(df["time"], unit="ms", utc=True).dt.floor("D")
        daily = df.groupby("dt", as_index=False).agg({"time":"last","close":"last"})
        return daily[["time","close"]]

    if status == 401:
        raise PermissionError(f"401 Unauthorized (no history for {coin_id})")

    if status == 429:
        # ONE more retry after waiting 5 s
        print(f"⚠️  {coin_id}: 429 Rate Limit (retry 1); sleeping 5 s…")
        time.sleep(5)
        r2 = requests.get(url, params=params, headers=HDRS, timeout=30)
        if r2.status_code == 200 and r2.headers.get("content-type","").startswith("application/json"):
            data = r2.json().get("prices", [])
            if not data:
                return pd.DataFrame(columns=["time","close"])
            df = pd.DataFrame(data, columns=["time","close"])
            df["time"] = df["time"].astype(int)
            df["dt"] = pd.to_datetime(df["time"], unit="ms", utc=True).dt.floor("D")
            daily = df.groupby("dt", as_index=False).agg({"time":"last","close":"last"})
            return daily[["time","close"]]
        if r2.status_code == 429:
            raise RuntimeError(f"429 persisted after retry for {coin_id}")
        r2.raise_for_status()

    # Any other 4xx/5xx
    r.raise_for_status()

# ─────────── MAIN ───────────
if __name__ == "__main__":
    # 1) Get the full USDT base list from CG
    print("⏳ Fetching ALL USDT‐quoted bases from Binance (via CoinGecko)…")
    all_bases = fetch_all_binance_usdt_bases()
    print(f"  → found {len(all_bases)} USDT bases on Binance")

    # 2) Exclude known stablecoins
    bases = [b for b in all_bases if b not in STABLES]
    print(f"  → after filtering stables: {len(bases)} tokens remain")

    if not bases:
        sys.exit("❌ No symbols left after filtering; aborting.")

    # 3) Build symbol→CoinGecko ID map
    print("⏳ Fetching CoinGecko /coins/list for ID mapping…")
    coin_list = fetch_coin_list()
    sym2id = build_symbol_to_id_map(coin_list)

    # 4) Prepare UNIX‐seconds timestamps
    start_ts = int(START.replace(tzinfo=dt.timezone.utc).timestamp())
    end_ts   = int(dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc).timestamp())

    rows = {}  # will accumulate {time_ms: {n, p75, p200}}

    # 5) Loop through each base
    for base in bases:
        coin_id = sym2id.get(base)
        if not coin_id:
            print(f"⚠️   {base} → no CoinGecko ID; skipping.")
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

        # Compute EMA-75 & EMA-200
        hist["ema75"]  = ta.trend.ema_indicator(hist["close"], 75)
        hist["ema200"] = ta.trend.ema_indicator(hist["close"], 200)
        hist["a75"]    = (hist["close"] > hist["ema75"]).astype(int)
        hist["a200"]   = (hist["close"] > hist["ema200"]).astype(int)

        # Tally
        for tm, a75, a200, e200 in zip(hist["time"], hist["a75"], hist["a200"], hist["ema200"]):
            if pd.isna(e200):
                continue
            if tm not in rows:
                rows[tm] = {"n":0, "p75":0, "p200":0}
            rows[tm]["n"]   += 1
            rows[tm]["p75"] += int(a75)
            rows[tm]["p200"]+= int(a200)

        time.sleep(PAUSE)

    # 6) Build final DataFrame
    if not rows:
        print("⚠️  No breadth data tallied; writing only headers.")
        daily = pd.DataFrame(columns=["time","pct75","pct200"])
    else:
        daily = pd.DataFrame({
            "time":   sorted(rows.keys()),
            "pct75":  [rows[t]["p75"]  / rows[t]["n"] * 100 for t in sorted(rows)],
            "pct200": [rows[t]["p200"] / rows[t]["n"] * 100 for t in sorted(rows)]
        })
        print(f"✅ Built daily DataFrame with {len(daily)} rows")

    # 7) Write CSVs for Pine‐Seeds
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
