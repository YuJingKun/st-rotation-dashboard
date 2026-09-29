#!/usr/bin/env python3
"""STSS cloud-only A-share daily quotes. Google Drive is the business-state owner.
GitHub is a technical market-data transport/cache, never a Court/Deal source.
Unadjusted ("day") prices only; suspended days are NOT forward-filled.
"""
import csv, json, math, os, random, time, urllib.parse, urllib.request
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

TZ = ZoneInfo("Asia/Shanghai")
NOW = datetime.now(TZ)
ROOT = Path("marketdata")
ROOT.mkdir(exist_ok=True)
TICKERS_PATH = ROOT / "stss_universe_snapshot.csv"
LATEST_PATH = ROOT / "stss_latest_50.csv"
PRICING_PATH = ROOT / "stss_pricing_50.csv"
HISTORY_PATH = ROOT / "stss_daily_history.csv"
QA_PATH = ROOT / "stss_market_qa.json"
COUNTERS = Counter()

with TICKERS_PATH.open(encoding="utf-8-sig", newline="") as f:
    POOL = [r["ticker"].strip() for r in csv.DictReader(f) if r.get("ticker")]
assert POOL and len(POOL) == len(set(POOL)), "empty or duplicate market-data snapshot"

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; STSS daily historical bars)",
           "Accept": "application/json,text/javascript,*/*"}
def fetch_json(url, referer, provider):
    error = None
    for attempt in range(2):
        try:
            req = urllib.request.Request(url, headers={**HEADERS, "Referer": referer})
            COUNTERS[provider] += 1
            with urllib.request.urlopen(req, timeout=14) as resp:
                obj = json.loads(resp.read(800000).decode("utf-8", errors="replace"))
            if not isinstance(obj, dict):
                raise ValueError("non-object JSON")
            return obj
        except Exception as exc:
            error = str(exc)[:180]
            if attempt == 0:
                time.sleep(0.8 + random.random() * 0.4)
    raise RuntimeError(error)

def cutoff_date():
    # In morning / during trading, never treat an intraday bar as a complete daily candle.
    if (NOW.hour, NOW.minute) < (15, 30):
        from datetime import timedelta
        return (NOW.date() - timedelta(days=1)).isoformat()
    return NOW.date().isoformat()

CUTOFF = cutoff_date()

def valid_bar(row):
    dt, ticker, op, hi, lo, cl, vol, amount, src = row
    nums = [op, hi, lo, cl]
    if not all(math.isfinite(float(x)) and float(x) > 0 for x in nums):
        raise ValueError("invalid OHLC " + ticker + " " + dt)
    if float(hi) + 0.011 < max(float(op), float(cl)):
        raise ValueError("invalid high " + ticker + " " + dt)
    if float(lo) - 0.011 > min(float(op), float(cl)):
        raise ValueError("invalid low " + ticker + " " + dt)
    if datetime.fromisoformat(dt).weekday() >= 5:
        raise ValueError("weekend bar " + dt)
    return row

def dedup_sort(rows):
    seen = set()
    output = []
    for row in rows:
        row = valid_bar(row)
        if row[0] > CUTOFF:
            continue
        key = (row[0], row[1])
        if key in seen:
            raise ValueError("duplicate ticker/date " + str(key))
        seen.add(key)
        output.append(row)
    return sorted(output, key=lambda r: r[0])

def tencent(ticker, limit=360):
    code, exchange = ticker.split(".")
    symbol = exchange.lower() + code
    url = "https://web.ifzq.gtimg.cn/appstock/app/fqkline/get?" + urllib.parse.urlencode({
        "param": symbol + ",day,,," + str(limit) + ",none"
    })
    obj = fetch_json(url, "https://gu.qq.com/", "Tencent_raw")
    d = (obj.get("data") or {}).get(symbol) or {}
    daily = d.get("day") or []
    if not daily:
        raise ValueError("Tencent raw day data absent; qfq not accepted")
    rows = []
    for b in daily:
        if len(b) < 6 or b[0] > CUTOFF:
            continue
        dt, op, cl, hi, lo, vol = b[:6]
        rows.append([dt, ticker, float(op), float(hi), float(lo), float(cl),
                     float(vol), None, "Tencent_raw"])
    return dedup_sort(rows)

def eastmoney(ticker, start="20250101"):
    code, exchange = ticker.split(".")
    secid = ("1" if exchange == "SH" else "0") + "." + code
    url = "https://push2his.eastmoney.com/api/qt/stock/kline/get?" + urllib.parse.urlencode({
        "secid": secid, "fields1": "f1,f2,f3,f4,f5,f6",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
        "klt": 101, "fqt": 0, "beg": start, "end": CUTOFF.replace("-", "")
    })
    obj = fetch_json(url, "https://quote.eastmoney.com/", "Eastmoney_raw")
    lines = (obj.get("data") or {}).get("klines") or []
    if not lines:
        raise ValueError("Eastmoney unadjusted klines absent")
    rows = []
    for line in lines:
        b = line.split(",")
        if len(b) < 7 or b[0] > CUTOFF:
            continue
        rows.append([b[0], ticker, float(b[1]), float(b[3]), float(b[4]),
                     float(b[2]), float(b[5]), float(b[6]), "Eastmoney_raw"])
    return dedup_sort(rows)

def get_bars(ticker):
    errors = []
    try:
        rows = tencent(ticker)
        if rows:
            return rows, errors
        errors.append("Tencent: empty")
    except Exception as exc:
        errors.append("Tencent: " + str(exc))
    try:
        rows = eastmoney(ticker)
        if rows:
            return rows, errors
        errors.append("Eastmoney: empty")
    except Exception as exc:
        errors.append("Eastmoney: " + str(exc))
    return [], errors

def read_csv(path):
    if not path.exists():
        return []
    with path.open(encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))

def write_csv(path, headers, rows):
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(rows)

# A trading session is current only when an actively traded benchmark has a complete bar.
bench = {}
for code in ("000001.SZ", "600000.SH"):
    try:
        b = tencent(code, 10)
        if not b:
            b = eastmoney(code)
        bench[code] = b[-1][0]
    except Exception as exc:
        bench[code] = "ERROR: " + str(exc)[:140]
good_bench = [x for x in bench.values() if len(x) == 10 and x[4] == "-" and x[7] == "-"]
if not good_bench:
    raise RuntimeError("No verified benchmark calendar. Preserve the last good market CSV.")
session = max(good_bench)
print("SESSION", session, "BENCHMARK", json.dumps(bench), flush=True)

results = {}
def worker(ticker):
    rows, errors = get_bars(ticker)
    return ticker, rows, errors
with ThreadPoolExecutor(max_workers=3) as executor:
    futures = [executor.submit(worker, ticker) for ticker in POOL]
    for future in as_completed(futures):
        ticker, rows, errors = future.result()
        results[ticker] = (rows, errors)
        print("TICKER", json.dumps({"ticker": ticker,
              "rows": len(rows),
              "date": rows[-1][0] if rows else None,
              "source": rows[-1][-1] if rows else None,
              "errors": errors}, ensure_ascii=False), flush=True)

# Three independent source checks; outage is a QA degradation, not a reason to
# fetch the entire pool a second time.
spot_codes = ("002360.SZ", "600491.SH", "000752.SZ")
spot = {}
for ticker in spot_codes:
    rows, _ = results.get(ticker, ([], []))
    sample = next((r for r in reversed(rows) if r[0] == session), None)
    if not sample or sample[-1] != "Tencent_raw":
        spot[ticker] = {"status": "NOT_ELIGIBLE"}
        continue
    try:
        comparison = eastmoney(ticker, start=session.replace("-", ""))
        other = next((r for r in comparison if r[0] == session), None)
        if not other:
            spot[ticker] = {"status": "SECONDARY_MISSING"}
        else:
            delta = abs(sample[5] - other[5])
            spot[ticker] = {"status": "PASS" if delta <= .011 else "PRICE_CONFLICT",
                            "tencent_close": sample[5],
                            "eastmoney_close": other[5],
                            "delta": delta}
    except Exception as exc:
        spot[ticker] = {"status": "SECONDARY_FAILED", "error": str(exc)[:160]}
    time.sleep(.45)

old_latest = {r["ticker"]: r for r in read_csv(LATEST_PATH)}
latest = []
pricing = []
fresh_rows = []
qa = []
generated = NOW.isoformat(timespec="seconds")
for ticker in POOL:
    bars, errors = results[ticker]
    r = bars[-1] if bars else None
    is_fresh = bool(r and r[0] == session)
    prior = old_latest.get(ticker)
    if not r and prior and prior.get("price_date"):
        # Cached price remains visible but explicitly fails the freshness gate.
        latest.append([prior.get("price_date"), ticker, prior.get("open", ""),
                       prior.get("high", ""), prior.get("low", ""),
                       prior.get("close", ""), prior.get("volume_raw", ""),
                       prior.get("amount_raw", ""), prior.get("source", ""),
                       "STALE_CACHED", session, "", "", "NOT_CHECKED", generated])
        pricing.append([ticker, prior.get("price_date"), prior.get("close", ""),
                        "", "", "", prior.get("source", ""), "STALE_CACHED", session])
    elif r:
        verification = spot.get(ticker, {}).get("status", "NOT_CHECKED")
        freshness = "LATEST" if is_fresh else "STALE_OR_HALT"
        if verification == "PRICE_CONFLICT":
            freshness = "PRICE_CONFLICT"
        latest.append([r[0], ticker, r[2], r[3], r[4], r[5],
                       "" if r[6] is None else r[6],
                       "" if r[7] is None else r[7], r[8], freshness, session,
                       "Eastmoney_raw" if verification in ("PASS", "PRICE_CONFLICT") else "",
                       spot.get(ticker, {}).get("eastmoney_close", ""),
                       verification, generated])
        returns = [round((r[5] / bars[-k-1][5] - 1) * 100, 7)
                   if is_fresh and len(bars) > k else "" for k in (1, 5, 20)]
        pricing.append([ticker, r[0], r[5], *returns,
                        r[8], freshness, session])
        if is_fresh and verification != "PRICE_CONFLICT":
            fresh_rows.append([r[0], ticker, r[2], r[3], r[4], r[5],
                               "" if r[6] is None else r[6],
                               "" if r[7] is None else r[7], r[8]])
    else:
        latest.append(["", ticker, "", "", "", "", "", "", "",
                       "UNAVAILABLE", session, "", "", "NOT_CHECKED", generated])
        pricing.append([ticker, "", "", "", "", "", "",
                        "UNAVAILABLE", session])
    qa.append({"ticker": ticker,
               "rows": len(bars),
               "latest_date": r[0] if r else None,
               "fresh": is_fresh,
               "source": r[8] if r else "UNAVAILABLE",
               "errors": errors})

assert len(latest) == len(POOL) == len(pricing), "coverage invariant"
if not any(r[9] == "LATEST" for r in latest):
    raise RuntimeError("No verified fresh daily bars; do not overwrite last-good output")

write_csv(LATEST_PATH, ["price_date","ticker","open","high","low","close",
    "volume_raw","amount_raw","source","freshness","expected_session",
    "secondary_source","secondary_close","crosscheck","generated_at_cst"], latest)
write_csv(PRICING_PATH, ["ticker","price_date","close","return_1d_pct",
    "return_5d_pct","return_20d_pct","source","freshness",
    "expected_session"], pricing)
old_hist = read_csv(HISTORY_PATH)
merged = {(r["date"], r["ticker"]): [r[h] for h in
          ("date","ticker","open","high","low","close","volume_raw",
           "amount_raw","source")] for r in old_hist if r.get("date") and r.get("ticker")}
for row in fresh_rows:
    merged[(row[0], row[1])] = row
write_csv(HISTORY_PATH, ["date","ticker","open","high","low","close",
           "volume_raw","amount_raw","source"],
          [merged[k] for k in sorted(merged)])
summary = {
    "generated_at_cst": generated, "expected_complete_session": session,
    "snapshot_source": "V6.2_CURRENT_STATE 50-ticker snapshot; Drive remains authoritative",
    "ticker_count": len(POOL),
    "fresh_count": sum(x[9] == "LATEST" for x in latest),
    "stale_count": sum(x[9] in ("STALE_CACHED","STALE_OR_HALT") for x in latest),
    "unavailable_count": sum(x[9] == "UNAVAILABLE" for x in latest),
    "price_conflict_count": sum(x[9] == "PRICE_CONFLICT" for x in latest),
    "benchmark": bench, "spot_crosschecks": spot,
    "request_counts": dict(COUNTERS),
    "status": "SUCCESS" if all(x[9] == "LATEST" for x in latest) else "DEGRADED",
    "per_ticker": qa,
    "no_future_bars": True, "raw_prices_only": True
}
QA_PATH.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
print("FINAL_SUMMARY", json.dumps({k:v for k,v in summary.items()
    if k != "per_ticker"}, ensure_ascii=False), flush=True)
