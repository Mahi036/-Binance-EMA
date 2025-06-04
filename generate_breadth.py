# generatebreadth.py
# ──────────────────────────────────────────────────────────────────────
import os
import sys
import time
import csv
import requests
import json
import pandas as pd
import ta
import datetime as dt

# ─────────── CONFIG ───────────
BINANCE_EXCHANGE_INFO = "https://api.binance.com/api/v3/exchangeInfo"
CC_HISTODAY           = "https://min-api.cryptocompare.com/data/v2/histoday"
CC_ALL_EXCHANGES      = "https://min-api.cryptocompare.com/data/all/exchanges"
API_KEY_ENV           = "CRYPTOCOMPARE_API_KEY"

# 1) We need ≥200 days of data before Jan 1 2024, so fetch from Jan 1 2023
EMA_WARMUP_START = dt.datetime(2023, 1, 1)   # warm-up start (UTC)
OUTPUT_START     = dt.datetime(2024, 1, 1)   # only output from this date onward (UTC)
PAUSE_MS         = 1200                     # 1.2 s between CryptoCompare calls
CONCURRENCY      = 4                        # up to 4 parallel CC calls

STABLES = {
    "USDT", "USDC", "BUSD", "DAI", "TUSD", "USDP", "USDD", "USTC", "FRAX", "FEI", "USX", "EURT"
}

# Fetch your CryptoCompare API key from environment
CC_API_KEY = os.getenv(API_KEY_ENV)
if not CC_API_KEY:
    sys.stderr.write(f"❌  Please set {API_KEY_ENV} in your environment.\n")
    sys.exit(1)

HEADERS_CC = {
    "Authorization": f"Apikey {CC_API_KEY}",
    "Accept": "application/json"
}

# ─────────── HELPERS ───────────

def fetch_usdt_bases_from_cc():
    """
    Uses CryptoCompare’s “all/exchanges?tsym=USDT” endpoint to find every coin
    that trades against USDT on Binance. Returns a sorted list of base symbols,
    excluding any stablecoins from STABLES. If the response format is unexpected
    or “Binance” section is missing, returns an empty list.
    """
    params = {
        "tsym": "USDT",
        "api_key": CC_API_KEY
    }

    try:
        r = requests.get(CC_ALL_EXCHANGES, params=params, headers=HEADERS_CC, timeout=30)
        r.raise_for_status()
    except requests.RequestException as e:
        print(f"❌ Error fetching exchange list from CryptoCompare: {e}")
        return []

    try:
        payload = r.json()
    except Exception as e:
        print(f"❌ Failed to parse JSON from CryptoCompare response: {e}")
        return []

    # Attempt to find “Data” → “Binance”
    data_section = payload.get("Data")
    if not isinstance(data_section, dict) or "Binance" not in data_section:
        print("⚠️  Unexpected CryptoCompare response format (no Data→Binance).")
        return []

    raw_bases = list(data_section["Binance"].keys())
    # Filter out stablecoins
    filtered = [b for b in raw_bases if b not in STABLES]
    return sorted(filtered)


def fetch_usdt_bases_from_binance():
    """
    Calls Binance /exchangeInfo and returns a list of base symbols
    where quoteAsset == "USDT" and baseAsset not in STABLES.
    Falls back to local data/exchangeInfo.json if the API call fails.
    """
    try:
        r = requests.get(BINANCE_EXCHANGE_INFO, timeout=15)
        r.raise_for_status()
        symbols = r.json().get("symbols", [])
    except requests.RequestException:
        # Fallback to local exchangeInfo data if available
        fallback_path = os.path.join("data", "exchangeInfo.json")
        if os.path.exists(fallback_path):
            with open(fallback_path, "r", encoding="utf-8") as f:
                try:
                    symbols = json.load(f).get("symbols", [])
                    print("⚠️  Using local exchangeInfo fallback data.")
                except Exception:
                    raise
        else:
            raise

    bases = []
    for s in symbols:
        if (
            s.get("status") == "TRADING"
            and s.get("isSpotTradingAllowed")
            and s.get("quoteAsset") == "USDT"
            and s.get("baseAsset") not in STABLES
        ):
            bases.append(s["baseAsset"])
    return sorted(set(bases))


def fetch_usdt_bases():
    """
    First attempts to fetch USDT bases from CryptoCompare. If that yields a
    non-empty list, returns it. Otherwise, falls back to Binance’s /exchangeInfo
    (with local fallback if necessary).
    """
    bases_cc = fetch_usdt_bases_from_cc()
    if bases_cc:
        print(f"  → Retrieved {len(bases_cc)} bases via CryptoCompare.")
        return bases_cc

    print("⚠️  Falling back to Binance /exchangeInfo for USDT bases.")
    try:
        bases_bn = fetch_usdt_bases_from_binance()
        print(f"  → Retrieved {len(bases_bn)} bases via Binance exchangeInfo.")
        return bases_bn
    except Exception as e:
        print(f"❌ Error fetching from Binance: {e}")
        return []


def fetch_history_from_cc(base):
    """
    Fetches daily OHLC for 'base' vs USD from CryptoCompare,
    from EMA_WARMUP_START up to today.

    Returns a DataFrame with columns ["time","close"] where time is ms‐since‐epoch (UTC midnight).
    If CryptoCompare returns an error (401/429), returns an empty DataFrame.
    """
    # Compute number of days between EMA_WARMUP_START and today
    start_dt = EMA_WARMUP_START.replace(tzinfo=dt.timezone.utc)
    end_dt   = dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    days_diff = (end_dt - start_dt).days

    to_ts = int(end_dt.timestamp())  # UNIX seconds at today midnight UTC

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
        print(f"  {base}USDT: 401 Unauthorized (no data); skipping.")
        return pd.DataFrame(columns=["time","close"])
    if status == 429:
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

    times  = [int(bar["time"]) * 1000 for bar in data]
    closes = [float(bar["close"]) for bar in data]

    df = pd.DataFrame({"time": times, "close": closes})
    return df


def pad_ema(arr, period):
    """
    Given a list of EMA values of length N - (period-1),
    return a list of length N where the first (period-1) entries are None,
    then the EMA values follow.
    """
    return [None] * (period - 1) + list(arr)


def write_pine_csv(path, header_name, series):
    """
    Writes a Pine-Seeds–style CSV at `path`, with columns:
      time, open, high, low, close, volume
    where open=high=low=close = series[i][header_name], volume=0.
    `series` is a list of dicts: [{"time": ms, "<header_name>": value}, …].
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
    print("⏳  Fetching USDT bases from CryptoCompare (with Binance fallback)…")
    bases = fetch_usdt_bases()
    print(f"  → {len(bases)} total non-stable USDT bases found.")

    if not bases:
        print("❌ No bases to process; exiting.")
        return

    # Container for breadth: { "YYYY-MM-DD": { a75:0, t75:0, a200:0, t200:0 } }
    breadth = {}

    # We'll fetch histories in parallel (up to CONCURRENCY threads), then pause PAUSE_MS after each
    from multiprocessing.pool import ThreadPool
    pool = ThreadPool(CONCURRENCY)

    def process_base(base):
        """
        Worker function for each base token.
        Fetch history, compute EMAs, and merge into breadth dict.
        """
        print(f"→ Fetching history for {base}USDT…")
        df = fetch_history_from_cc(base)

        if df.empty or len(df) < 200:
            print(f"  {base}USDT: only {len(df)} days (<200), skipping.")
            return

        closes = df["close"].tolist()
        # Calculate EMA-75 & EMA-200
        ema75_arr  = pad_ema(
            ta.trend.ema_indicator(pd.Series(closes), 75).tolist()[74:],
            75,
        )
        ema200_arr = pad_ema(
            ta.trend.ema_indicator(pd.Series(closes), 200).tolist()[199:],
            200,
        )

        # Convert each bar's "time" (ms) → date string "YYYY-MM-DD" (UTC)
        dates = [dt.datetime.utcfromtimestamp(t // 1000).strftime("%Y-%m-%d") for t in df["time"]]

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

        # Pause between CryptoCompare calls
        time.sleep(PAUSE_MS / 1000.0)

    # Launch threads
    pool.map(process_base, bases)
    pool.close()
    pool.join()

    # Now build the final series, but only include dates ≥ OUTPUT_START
    out_start_str = OUTPUT_START.strftime("%Y-%m-%d")
    all_dates = sorted(date for date in breadth.keys() if date >= out_start_str)

    series75  = []
    series200 = []
    for date in all_dates:
        entry = breadth[date]
        t_ms = int(dt.datetime.strptime(date, "%Y-%m-%d").replace(
            tzinfo=dt.timezone.utc
        ).timestamp() * 1000)

        pct75  = (entry["a75"]  / entry["t75"]  * 100.0) if entry["t75"] > 0  else 0.0
        pct200 = (entry["a200"] / entry["t200"] * 100.0) if entry["t200"] > 0 else 0.0

        series75.append({"time": t_ms, "pct_above_75":  round(pct75, 2)})
        series200.append({"time": t_ms, "pct_above_200": round(pct200, 2)})

    # Write CSVs
    write_pine_csv("data/BR75.csv",  "pct_above_75",  series75)
    write_pine_csv("data/BR200.csv", "pct_above_200", series200)

    print(f"💾  data/BR75.csv  ({len(series75)} rows starting {out_start_str})")
    print(f"💾  data/BR200.csv ({len(series200)} rows starting {out_start_str})")


if __name__ == "__main__":
    main()
