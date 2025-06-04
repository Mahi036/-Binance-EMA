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

STABLES = {# generatebreadth.py
# ─────────────────────────────────────────────────────────────────────
import os
import sys
import time
import csv
import requests
import pandas as pd
import ta
import datetime as dt
import dayjs   # You can install dayjs via pip: pip install python-dayjs

# ─────────── CONFIG ───────────
BINANCE_EXCHANGE_INFO = "https://api.binance.com/api/v3/exchangeInfo"
CC_HISTODAY           = "https://min-api.cryptocompare.com/data/v2/histoday"
API_KEY_ENV           = "CRYPTOCOMPARE_API_KEY"

# 1) To compute EMA-75/EMA-200 starting Jan 1 2024, we need ~200 days of warmup → fetch from Jan 1 2023
EMA_WARMUP_START = dt.datetime(2023, 1, 1)   # warm-up start
OUTPUT_START     = dt.datetime(2024, 1, 1)   # only output from this date onward
PAUSE_MS         = 1200                     # 1.2 s between CryptoCompare calls (~0.8 req/sec, under CC’s 10 req/sec free limit)
CONCURRENCY      = 4                        # up to 4 parallel CC calls
STABLES = {
    "USDT","USDC","BUSD","DAI","TUSD","USDP","USDD","USTC","FRAX","FEI","USX","EURT"
}

# Fetch your CryptoCompare API key from the environment
CC_API_KEY = os.getenv(API_KEY_ENV)
if not CC_API_KEY:
    sys.stderr.write(f"❌  Please set {API_KEY_ENV} in your environment.\n")
    sys.exit(1)

HEADERS_CC = {
    "Authorization": f"Apikey {CC_API_KEY}",
    "Accept": "application/json"
}

# ─────────── HELPERS ───────────

def fetch_usdt_bases_from_binance():
    """
    Calls Binance /exchangeInfo and returns a list of base symbols
    where quoteAsset == "USDT" and baseAsset not in STABLES.
    """
    r = requests.get(BINANCE_EXCHANGE_INFO, timeout=15)
    r.raise_for_status()
    symbols = r.json().get("symbols", [])
    bases = []
    for s in symbols:
        if (
            s.get("status") == "TRADING" and
            s.get("isSpotTradingAllowed") and
            s.get("quoteAsset") == "USDT" and
            s.get("baseAsset") not in STABLES
        ):
            bases.append(s["baseAsset"])
    return sorted(set(bases))


def fetch_history_from_cc(base):
    """
    Fetches daily OHLC for 'base' vs USD from CryptoCompare,
    from EMA_WARMUP_START up to today.

    Returns a DataFrame with columns ["time","close"] where time is ms‐since‐epoch (UTC midnight).
    If CryptoCompare returns an error (401/429), returns an empty DataFrame.
    """
    # Compute number of days between EMA_WARMUP_START and today
    start_dt = EMA_WARMUP_START.replace(tzinfo=dt.timezone.utc)
    end_dt   = dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc).replace(hour=0,minute=0,second=0,microsecond=0)
    days_diff = (end_dt - start_dt).days

    to_ts = int(end_dt.timestamp())  # UNIX seconds

    params = {
        "fsym": base,
        "tsym": "USD",
        "toTs": to_ts,
        "limit": days_diff,
        "api_key": CC_API_KEY
    }

    try:
        resp = requests.get(CC_HISTODAY, params=params, headers=HEADERS_CC, timeout=30)
    except requests.RequestException as e:
        print(f"  {base}USDT: request error {e}; skipping.")
        return pd.DataFrame(columns=["time","close"])

    status = resp.status_code
    if status == 401:
        # no data available for this symbol
        print(f"  {base}USDT: 401 Unauthorized (no data); skipping.")
        return pd.DataFrame(columns=["time","close"])
    if status == 429:
        # rate‐limited
        print(f"  {base}USDT: 429 Rate Limit; skipping.")
        return pd.DataFrame(columns=["time","close"])
    try:
        data = resp.json().get("Data", {}).get("Data", [])
    except Exception:
        print(f"  {base}USDT: invalid JSON response; skipping.")
        return pd.DataFrame(columns=["time","close"])

    if not isinstance(data, list) or not data:
        print(f"  {base}USDT: empty data; skipping.")
        return pd.DataFrame(columns=["time","close"])

    # Build a DataFrame: for each bar, "time" is UNIX seconds → convert to ms
    times  = [int(bar["time"]) * 1000 for bar in data]
    closes = [float(bar["close"]) for bar in data]

    df = pd.DataFrame({"time": times, "close": closes})
    return df


def pad_ema(arr, period):
    """
    Given a 1D array of EMA values (length = N - (period-1)),
    return an array of length N where the first (period-1) entries are None,
    then the EMA values follow.
    """
    return [None] * (period - 1) + list(arr)


def write_pine_csv(path, header_name, series):
    """
    series is a list of dicts: [ {"time": ms, "<header_name>": value}, … ]
    Writes CSV with columns: time, open, high, low, close, volume
    where open=high=low=close=series[i][header_name], volume=0.
    """
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["time", "open", "high", "low", "close", "volume"])
        for row in series:
            t = row["time"]
            val = row[header_name]
            writer.writerow([t, val, val, val, val, 0])


# ─────────── MAIN ───────────

def main():
    print("⏳  Fetching USDT bases from Binance…")
    bases = fetch_usdt_bases_from_binance()
    print(f"  → {len(bases)} non-stable USDT bases found.")

    # Container for breadth: { "YYYY-MM-DD": { a75:0,t75:0, a200:0,t200:0 } }
    breadth = {}

    # We will fetch histories in parallel but respect PAUSE_MS after each fetch
    from multiprocessing.pool import ThreadPool
    pool = ThreadPool(CONCURRENCY)

    def process_base(base):
        """
        Worker function for a single base token.
        Fetch its history, compute EMAs, and merge into breadth dict.
        """
        print(f"→ Fetching history for {base}USDT…")
        df = fetch_history_from_cc(base)

        if df.empty or len(df) < 200:
            print(f"  {base}USDT: only {len(df)} days (<200), skipping.")
            return

        # Compute EMA-75 & EMA-200 on the close series
        closes = df["close"].tolist()
        ema75_arr  = pad_ema(ta.trend.ema_indicator(pd.Series(closes), 75).tolist(), 75)
        ema200_arr = pad_ema(ta.trend.ema_indicator(pd.Series(closes), 200).tolist(), 200)

        # Convert each bar's "time" (ms) → date string YYYY-MM-DD
        dates = [dt.datetime.utcfromtimestamp(t // 1000).strftime("%Y-%m-%d") for t in df["time"]]

        # Tally breadth
        for i, date in enumerate(dates):
            e75  = ema75_arr[i]
            e200 = ema200_arr[i]
            c    = closes[i]

            if date not in breadth:
                breadth[date] = {"a75": 0, "t75": 0, "a200": 0, "t200": 0}

            if e75 is not None:
                breadth[date]["t75"] += 1
                if c > e75:
                    breadth[date]["a75"] += 1

            if e200 is not None:
                breadth[date]["t200"] += 1
                if c > e200:
                    breadth[date]["a200"] += 1

        # Pause to stay under CryptoCompare’s rate limit
        time.sleep(PAUSE_MS / 1000.0)

    # Launch threads
    pool.map(process_base, bases)
    pool.close()
    pool.join()

    # Build two series (date ascending) but only for dates ≥ OUTPUT_START
    out_start_str = OUTPUT_START.strftime("%Y-%m-%d")
    all_dates = sorted(date for date in breadth.keys() if date >= out_start_str)

    series75 = []
    series200 = []
    for date in all_dates:
        entry = breadth[date]
        t_ms = int(dt.datetime.strptime(date, "%Y-%m-%d").replace(tzinfo=dt.timezone.utc).timestamp() * 1000)

        pct75  = (entry["a75"]  / entry["t75"]  * 100.0) if entry["t75"] > 0  else 0.0
        pct200 = (entry["a200"] / entry["t200"] * 100.0) if entry["t200"] > 0 else 0.0

        series75.append({ "time": t_ms, "pct_above_75":  round(pct75, 2) })
        series200.append({ "time": t_ms, "pct_above_200": round(pct200, 2) })

    # Write CSVs
    write_pine_csv("data/BR75.csv",  "pct_above_75",  series75)
    write_pine_csv("data/BR200.csv", "pct_above_200", series200)

    print(f"💾  data/BR75.csv  ({len(series75)} rows starting {out_start_str})")
    print(f"💾  data/BR200.csv ({len(series200)} rows starting {out_start_str})")


if __name__ == "__main__":
    main()

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
