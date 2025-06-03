import pandas as pd, requests, ta, csv, datetime as dt, os, sys, time, random

# ---------- CONFIG -------------------------------------------------
EXCHANGEINFO_SOURCES = [
    # raw daily snapshot (JSON file) – never rate-limited
    "https://raw.githubusercontent.com/binance/binance-spot-api-docs/master/endpoints/exchangeInfo.json",
    # live APIs (try until one works)
    "https://api1.binance.com/api/v3/exchangeInfo",
    "https://api2.binance.com/api/v3/exchangeInfo",
    "https://api3.binance.com/api/v3/exchangeInfo"
]

KLINE_BASES = [
    "https://api1.binance.com",
    "https://api2.binance.com",
    "https://api3.binance.com"
]

START   = datetime.datetime(2023, 1, 1)
STABLES = {
    'USDT','USDC','FDUSD','TUSD','DAI','USDP','BUSD','USDD',
    'AEUR','XUSD','USD1','PYUSD','PAXG','WBTC','WBETH'
}
HDRS = {"User-Agent": "Mozilla/5.0 (GitHub Actions bot)"}

# ---------- HELPERS ------------------------------------------------
def get_json_with_retry(urls, max_try=5):
    for url in urls:
        for n in range(max_try):
            try:
                r = requests.get(url, headers=HDRS, timeout=15)
                if r.ok and r.headers.get("content-type","").startswith("application/json"):
                    return r.json()
                print(f"⚠️  [{n+1}] non-JSON {r.status_code} from {url}")
            except requests.exceptions.RequestException as e:
                print(f"⚠️  [{n+1}] {e} for {url}")
            time.sleep(2+random.random())
    sys.exit("❌  all sources failed")

def universe():
    data = get_json_with_retry(EXCHANGEINFO_SOURCES)
    return [
        s["symbol"] for s in data["symbols"]
        if s["status"] == "TRADING"
        and s["isSpotTradingAllowed"]
        and s["quoteAsset"] == "USDT"
        and s["baseAsset"] not in STABLES
    ]

def klines(sym, start_ms):
    for base in KLINE_BASES:
        try:
            out, frm = [], start_ms
            while True:
                r = requests.get(f"{base}/api/v3/klines",
                                 params={"symbol":sym,"interval":"1d","startTime":frm,"limit":1000},
                                 headers=HDRS, timeout=15)
                if not (r.ok and r.headers.get("content-type","").startswith("application/json")):
                    raise ValueError(f"bad response {r.status_code}")
                d = r.json()
                if not d: break
                out += d
                frm = d[-1][0] + 86_400_000
                if len(d) < 1000: break
            return pd.DataFrame({"time":[int(k[0]) for k in out],
                                 "close":[float(k[4]) for k in out]})
        except Exception as e:
            print(f"⚠️  {sym}: {e} on {base}, trying next host…")
    sys.exit(f"❌  all kline hosts failed for {sym}")


def tv_write(col, out_fn, df):
    os.makedirs("data", exist_ok=True)
    with open(out_fn, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["time","open","high","low","close","volume"])
        for t,v in zip(df["time"], df[col]):
            w.writerow([t,v,v,v,v,0])

print("⏳ Building universe…")
syms = universe()
print("Universe size:", len(syms))

rows = {}
start_ms = int(START.timestamp()*1000)

for sym in syms:
    df = klines(sym, start_ms)
    if len(df) < 200: continue
    df["ema75"]  = ta.trend.ema_indicator(df["close"], 75)
    df["ema200"] = ta.trend.ema_indicator(df["close"], 200)
    df["a75"]  = df["close"] > df["ema75"]
    df["a200"] = df["close"] > df["ema200"]
    for t,a75,a200,e200 in zip(
            df["time"], df["a75"], df["a200"], df["ema200"]):
        if pd.isna(e200): continue          # skip warm-up
        if t not in rows: rows[t] = {"n":0,"p75":0,"p200":0}
        rows[t]["n"]   += 1
        rows[t]["p75"] += a75
        rows[t]["p200"]+= a200

daily = pd.DataFrame({
    "time": sorted(rows.keys()),
    "pct75": [rows[t]["p75"]/rows[t]["n"]*100 for t in sorted(rows)],
    "pct200":[rows[t]["p200"]/rows[t]["n"]*100 for t in sorted(rows)]
})

tv_write("pct75",  "data/BR75.csv",  daily)
tv_write("pct200", "data/BR200.csv", daily)
print("✅ CSVs written to data/")
