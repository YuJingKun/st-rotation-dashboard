#!/usr/bin/env python3
"""STSS one-time bulk historic backfill + independent 200-ticker cloud stress test.

No Tushare pricing dependency. Tencent prefers RAW daily prices; explicit qfq fallback is labelled;
Eastmoney is a sparse cross-check / automatic fallback only. All validation failures
are explicitly recorded; missing sessions must not be fabricated.
"""
import csv, json, math, random, re, time, urllib.parse, urllib.request
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
from pathlib import Path

ROOT = Path("marketdata/archive_20260928")
ROOT.mkdir(parents=True, exist_ok=True)
END, START, STRESS_START = "2026-09-28", "2025-09-01", "2026-04-01"
POOL = json.loads('["000010.SZ","000610.SZ","000632.SZ","000669.SZ","000826.SZ","000838.SZ","000972.SZ","002168.SZ","002360.SZ","002528.SZ","002547.SZ","002620.SZ","002634.SZ","002726.SZ","002743.SZ","002789.SZ","002856.SZ","300027.SZ","300044.SZ","300068.SZ","300123.SZ","300147.SZ","300198.SZ","300212.SZ","300301.SZ","300338.SZ","300385.SZ","300472.SZ","300477.SZ","300716.SZ","301030.SZ","600180.SH","600337.SH","600340.SH","600491.SH","600537.SH","600881.SH","603377.SH","603378.SH","603398.SH","603843.SH","603959.SH","688033.SH","000752.SZ","000829.SZ","002514.SZ","300205.SZ","300460.SZ","002542.SZ","000056.SZ"]')
HEADERS = {"User-Agent":"Mozilla/5.0 (compatible; STSS data quality)",
           "Accept":"application/json, text/javascript, */*"}
REQUESTS = Counter()
def request_json(url, referer, label):
    err = None
    for attempt in range(3):
        try:
            req = urllib.request.Request(url, headers={**HEADERS,"Referer":referer})
            REQUESTS[label] += 1
            with urllib.request.urlopen(req,timeout=15) as h:
                data=h.read(850000)
            obj=json.loads(data.decode("utf-8",errors="replace"))
            if not isinstance(obj, dict):
                raise ValueError("not JSON object")
            return obj
        except Exception as e:
            err = str(e)[:170]
            time.sleep((attempt+1)*0.9 + random.random()*.45)
    raise RuntimeError(err)
def safe_num(x):
    try:
        n=float(x)
        return n if math.isfinite(n) else None
    except (TypeError, ValueError): return None
def tencent(ticker,limit=450,start=START):
    code, exchange = ticker.split(".")
    symbol=exchange.lower()+code
    # Tencent's fqkline endpoint may require an explicit adjustment flag.
    # Prefer raw 'none'. If unavailable, use qfq ONLY with source labelling.
    daily=[];adjustment=None;last_error=None
    for mode in ("none","qfq"):
        url=("https://web.ifzq.gtimg.cn/appstock/app/fqkline/get?"
             +urllib.parse.urlencode({"param":symbol+",day,,,"+str(limit)+","+mode}))
        try:
            obj=request_json(url,"https://gu.qq.com/","tencent")
            d=(obj.get("data") or {}).get(symbol) or {}
            daily=d.get("day") or d.get("qfqday") or []
            if daily:
                adjustment="raw" if mode=="none" and d.get("day") else "qfq"
                break
        except Exception as e:
            last_error=str(e)
    if not daily:
        raise ValueError("Tencent returned no raw or qfq history: "+str(last_error))
    bars=[]
    for x in daily:
        if len(x)<6 or not start<=x[0]<=END: continue
        dt,op,cl,hi,lo,vol=x[:6]
        op,cl,hi,lo,vol=map(safe_num,[op,cl,hi,lo,vol])
        if None in (op,cl,hi,lo) or min(op,cl,hi,lo)<=0 or hi+0.011<max(op,cl) or lo-0.011>min(op,cl):
            raise ValueError("bad OHLC "+str(x[:6]))
        bars.append([dt,ticker,op,hi,lo,cl,vol,None,"Tencent_"+adjustment])
    return validate(bars)
def eastmoney(ticker,start=START):
    code,exchange=ticker.split(".")
    sec=("1" if exchange=="SH" else "0")+"."+code
    url=("https://push2his.eastmoney.com/api/qt/stock/kline/get?"
         +urllib.parse.urlencode({"secid":sec,
           "fields1":"f1,f2,f3,f4,f5,f6",
           "fields2":"f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
           "klt":"101","fqt":"0","beg":start.replace("-",""),
           "end":END.replace("-","")}))
    obj=request_json(url,"https://quote.eastmoney.com/","eastmoney")
    daily=(obj.get("data") or {}).get("klines") or []
    if not daily:raise ValueError("Eastmoney raw klines empty")
    bars=[]
    for x in daily:
        a=x.split(",")
        if len(a)<8 or not start<=a[0]<=END:continue
        op,cl,hi,lo,vol,amount=map(safe_num,[a[1],a[2],a[3],a[4],a[5],a[6]])
        if None in (op,cl,hi,lo) or min(op,cl,hi,lo)<=0 or hi+0.011<max(op,cl) or lo-0.011>min(op,cl):
            raise ValueError("bad Eastmoney OHLC "+str(a[:8]))
        bars.append([a[0],ticker,op,hi,lo,cl,vol,amount,"Eastmoney_raw"])
    return validate(bars)
def validate(bars):
    seen=set()
    for x in bars:
        if x[0] in seen:raise ValueError("duplicate ticker/date "+x[0])
        seen.add(x[0])
        date.fromisoformat(x[0])
        if date.fromisoformat(x[0]).weekday()>=5:raise ValueError("weekend bar "+x[0])
    return sorted(bars)
def acquire(ticker,limit=450,start=START,fallback=False):
    errors=[]
    try:
        arr=tencent(ticker,limit,start)
        if arr:return arr,errors
        errors.append("Tencent returned empty")
    except Exception as e:errors.append("Tencent: "+str(e))
    if fallback:
        try:
            arr=eastmoney(ticker,start)
            if arr:return arr,errors
        except Exception as e:errors.append("Eastmoney: "+str(e))
    return [],errors
t0=time.monotonic()
print("BACKFILL_START",json.dumps({"pool":len(POOL),"start":START,"end":END}),flush=True)
# Use two actively traded benchmark codes to measure the historical-session calendar.
refs={}
for ticker in ["000001.SZ","600000.SH"]:
    arr,errs=acquire(ticker,450,START,True)
    refs[ticker]=[x[0] for x in arr]
    print("REFERENCE",ticker,len(arr),errs,flush=True)
calendar=set(refs.get("000001.SZ",[]))|set(refs.get("600000.SH",[]))
assert calendar, "Both benchmark calendars unavailable; cannot validate completeness"
def pool_job(ticker):
    bars, errors=acquire(ticker,450,START,True)
    dates=set(x[0] for x in bars)
    return ticker,bars,{"ticker":ticker,"rows":len(bars),
      "first":bars[0][0] if bars else None,
      "last":bars[-1][0] if bars else None,
      "latest_session_present":END in dates,
      "benchmark_missing_sessions":sorted(calendar-dates),
      "source":bars[0][-1] if bars else "UNAVAILABLE",
      "errors":errors}
pool_res={}
with ThreadPoolExecutor(max_workers=3) as ex:
    fut={ex.submit(pool_job,t):t for t in POOL}
    for future in as_completed(fut):
        ticker,bars,status=future.result()
        pool_res[ticker]=(bars,status)
        print("POOL",json.dumps({k:v for k,v in status.items() if k!="benchmark_missing_sessions"},
                                 ensure_ascii=False),flush=True)
parts=[]
header=["date","ticker","open","high","low","close","volume_raw","amount_raw","source"]
all_bars=[]
for i in range(0,len(POOL),10):
    group=POOL[i:i+10]
    lines=[row for ticker in group for row in pool_res[ticker][0]]
    lines.sort(key=lambda x:(x[1],x[0]))
    path=ROOT/("part_%02d.csv"%(i//10+1))
    with path.open("w",newline="",encoding="utf-8") as fp:
        w=csv.writer(fp);w.writerow(header)
        for r in lines:
            w.writerow([("" if x is None else ("%.4f"%x if isinstance(x,float) else x))
                        for x in r])
    all_bars.extend(lines)
    parts.append({"path":str(path),"tickers":group,"rows":len(lines)})
last50=[]
price50=[]
for ticker in POOL:
    a,status=pool_res[ticker]
    if a:
        r=a[-1]
        last50.append([r[0],ticker,r[5],r[-1],int(r[0]==END),r[7]])
        vals=[r[5]/a[-i-1][5]*100-100 if len(a)>i else None for i in (1,5,20)]
        price50.append([ticker,r[0],r[5],*vals,r[-1],"LATEST" if r[0]==END else "STALE"])
    else:
        last50.append(["",ticker,"","UNAVAILABLE",0,""])
        price50.append([ticker,"","","","","","UNAVAILABLE","NO_DATA"])
def save_csv(path,head,rows):
    with open(path,"w",encoding="utf-8",newline="") as f:
        w=csv.writer(f);w.writerow(head);w.writerows(rows)
save_csv("marketdata/stss_latest_50_20260928.csv",
         ["date","ticker","close","source","has_20260928","amount_raw"],last50)
save_csv("marketdata/stss_pricing_50_20260928.csv",
         ["ticker","price_date","close","return_1d_pct","return_5d_pct","return_20d_pct","source","freshness"],price50)
# One-time market-universe generation. BaoStock is used for SYMBOL METADATA only;
# all stress-test and backfill PRICES are retrieved over public HTTP from Tencent.
roster_err=None; extras=[]; listed=[]
try:
    import baostock as bs
    lg=bs.login()
    if str(lg.error_code)!="0":raise RuntimeError(lg.error_msg)
    try:
        rs=bs.query_all_stock(day=END)
        if str(rs.error_code)!="0":raise RuntimeError(rs.error_msg)
        while rs.next():
            x=dict(zip(rs.fields,rs.get_row_data()))
            code=x.get("code","")
            if not re.match(r"^(sh\.(60|68)|sz\.(00|30))\d+$",code):continue
            exchange, digits=code.split(".")
            ticker=digits+"."+exchange.upper()
            name=x.get("code_name","")
            listed.append((ticker,name))
    finally:bs.logout()
except Exception as e:
    roster_err=str(e)[:200]
inpool=set(POOL)
st=[x for x in listed if x[0] not in inpool and "ST" in x[1].upper()]
nonst=[x for x in listed if x[0] not in inpool and "ST" not in x[1].upper()]
extras=[x[0] for x in (st+nonst)[:150]]
stress_roster=POOL+extras
print("STRESS_ROSTER",json.dumps({"pool":len(POOL),"additional":len(extras),
     "total":len(stress_roster),"st_additional":min(150,len(st)),
     "roster_error":roster_err}),flush=True)
# For 50 production tickers, reuse the already fetched bars: NO duplicate HTTP traffic.
stress_status=[]
for ticker in POOL:
    arr=pool_res[ticker][0]
    trimmed=[x for x in arr if x[0]>=STRESS_START]
    stress_status.append({"ticker":ticker,"rows":len(trimmed),
       "latest_session_present":END in {x[0] for x in trimmed},
       "origin":"pool_reuse","error":pool_res[ticker][1]["errors"]})
def stress_job(ticker):
    arr,errors=acquire(ticker,190,STRESS_START,False)
    return {"ticker":ticker,"rows":len(arr),
        "first":arr[0][0] if arr else None,
        "last":arr[-1][0] if arr else None,
        "latest_session_present":END in {x[0] for x in arr},
        "origin":arr[0][-1] if arr else "UNAVAILABLE","error":errors}
if extras:
    with ThreadPoolExecutor(max_workers=3) as ex:
        futures={ex.submit(stress_job,t):t for t in extras}
        for f in as_completed(futures):
            d=f.result()
            stress_status.append(d)
            if d["error"] or not d["latest_session_present"]:
                print("STRESS_ISSUE",json.dumps(d,ensure_ascii=False),flush=True)
sample_refs={"000752.SZ":8.96,"002360.SZ":5.50,
 "600491.SH":1.62,"002168.SZ":3.59,"002634.SZ":4.04,
 "300212.SZ":7.70,"000610.SZ":7.40,"002856.SZ":17.79,
 "002542.SZ":1.63,"300205.SZ":5.36}
ref_validation=[]
for ticker,expected in sample_refs.items():
    r=next((x for x in pool_res[ticker][0] if x[0]=="2026-09-24"),None)
    ref_validation.append({"ticker":ticker,"expected":expected,
      "actual":r[5] if r else None,
      "pass":bool(r and abs(r[5]-expected)<0.011)})
# Raw Eastmoney spot-check only, avoid provider block from bulk calls.
east_checks=[]
for t in ["002360.SZ","600491.SH","000752.SZ"]:
    try:
        e=eastmoney(t,"2026-09-20")
        last=next((r for r in e if r[0]==END),None)
        other=next((r for r in pool_res[t][0] if r[0]==END),None)
        east_checks.append({"ticker":t,"eastmoney":last[5] if last else None,
          "tencent":other[5] if other else None,
          "same":bool(last and other and abs(last[5]-other[5])<0.011)})
    except Exception as err:
        east_checks.append({"ticker":t,"error":str(err)[:150]})
summary={
  "run_type":"STSS_WEB_HTTP_FULL_BACKFILL_AND_STRESS",
  "window":{"start":START,"end":END},
  "reference_calendar":{"total":len(calendar),
      "first":min(calendar),"last":max(calendar),
      "benchmark_sizes":{k:len(v) for k,v in refs.items()}},
  "pool":{"target_count":len(POOL),
      "success_with_history":sum(bool(v[0]) for v in pool_res.values()),
      "latest_complete_date_count":sum(v[1]["latest_session_present"] for v in pool_res.values()),
      "total_bars":len(all_bars),"files":parts,
      "tickers":[pool_res[t][1] for t in POOL],
      "reference_close_20260924":ref_validation},
  "stress":{"distinct_target":len(stress_roster),
      "additional":len(extras),
      "successful_history":sum(s["rows"]>0 for s in stress_status),
      "latest_date_count":sum(s["latest_session_present"] for s in stress_status),
      "total_bars":sum(s["rows"] for s in stress_status),
      "roster_error":roster_err,
      "results":sorted(stress_status,key=lambda s:s["ticker"])},
  "eastmoney_crosschecks":east_checks,
  "elapsed_seconds":round(time.monotonic()-t0,2)
}
(ROOT/"summary.json").write_text(json.dumps(summary,ensure_ascii=False,indent=2),
                                 encoding="utf-8")
print("FINAL_SUMMARY",json.dumps({"reference_days":len(calendar),
  "pool":{k:summary["pool"][k] for k in ("target_count","success_with_history",
                                          "latest_complete_date_count","total_bars")},
  "stress":{k:summary["stress"][k] for k in ("distinct_target","additional",
                                            "successful_history","latest_date_count",
                                            "total_bars","roster_error")},
  "spot_checks":east_checks,
  "reference_10_ok":sum(x["pass"] for x in ref_validation),
  "elapsed_seconds":summary["elapsed_seconds"]},ensure_ascii=False),flush=True)
