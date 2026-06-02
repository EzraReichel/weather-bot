#!/usr/bin/env python3
"""
scripts/verify_backtest_data.py -- Phase 1: Confirm backtest data availability.

Checks three things before building the full backtest harness:

  1. Kalshi historical market API
     Find a settled NYC high-temp market from ~4 months ago and confirm the
     /historical/markets/{ticker} endpoint returns a 'result' field and
     settlement metadata. Also test candlestick availability.

  2. Open-Meteo Previous Runs API
     For the same target_date, hit the Previous Runs API and confirm we get
     sane temperature values at 1-, 2-, and 3-day lead times for GFS, ECMWF,
     and GEM separately. NOTE: the API uses HOURLY temperature_2m_previous_dayN
     (not daily max/min) -- we derive daily max from the 24 hourly values.

  3. Ensemble member availability (CRITICAL)
     The live model uses ensemble_fraction (70% weight) which requires
     per-member values. Test whether the Previous Runs API returns per-member
     data or only ensemble means. If only means: backtest is blocked.

Run:
    cd /path/to/weather-bot && source .venv/bin/activate
    python scripts/verify_backtest_data.py

Requires KALSHI_API_KEY_ID and KALSHI_PRIVATE_KEY_PATH (or _PEM) in .env.
"""

import asyncio
import json
import re
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import httpx
from dotenv import load_dotenv

load_dotenv()

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from backend.data.kalshi_client import KalshiClient, kalshi_credentials_present

# -- Constants ----------------------------------------------------------
# ~4.5 months ago: clearly inside historical territory (before Feb 2026 cutoff)
TARGET_DATE = date(2025, 12, 15)

NYC = {"lat": 40.7789, "lon": -73.9692, "name": "NYC (KNYC/Central Park)"}

PREV_RUNS_URL = "https://previous-runs-api.open-meteo.com/v1/forecast"
ENSEMBLE_URL  = "https://ensemble-api.open-meteo.com/v1/ensemble"

KALSHI_SERIES_TO_TRY = [
    ("KXHIGHNY",  "nyc",     "high"),
    ("KXHIGHCHI", "chicago", "high"),
    ("KXHIGHDEN", "denver",  "high"),
]


# =====================================================================
# SECTION 1: Kalshi Historical API
# =====================================================================

async def check_kalshi_historical(client: KalshiClient) -> Dict[str, Any]:
    results: Dict[str, Any] = {
        "historical_cutoff": None,
        "ticker_found": None,
        "result_field": None,
        "settlement_metadata": {},
        "candlesticks_available": None,
        "candlestick_ask": None,
        "errors": [],
    }

    # 1a. Historical cutoff
    print("\n  [1a] Fetching historical cutoff...")
    cutoff_date: Optional[date] = None
    try:
        cutoff_data = await client.get("/historical/cutoff")
        # Kalshi returns {"market_settled_ts": "2026-02-27T...", ...}
        cutoff_raw = (
            cutoff_data.get("market_settled_ts")
            or cutoff_data.get("cutoff_timestamp")
            or cutoff_data.get("cutoff")
        )
        if cutoff_raw:
            cutoff_dt = datetime.fromisoformat(str(cutoff_raw).replace("Z", "+00:00"))
            cutoff_date = cutoff_dt.date()
            results["historical_cutoff"] = cutoff_dt.isoformat()
            in_hist = TARGET_DATE < cutoff_date
            print(f"       market_settled_ts = {cutoff_dt.strftime('%Y-%m-%d')} UTC")
            print(f"       TARGET_DATE {TARGET_DATE} is {'IN HISTORICAL territory' if in_hist else 'NOT in historical territory'}")
        else:
            print(f"       Full response (no recognized key): {json.dumps(cutoff_data)[:400]}")
    except Exception as e:
        err = f"historical/cutoff failed: {e}"
        results["errors"].append(err)
        print(f"       ERROR: {err}")

    # 1b. Find a settled ticker from ~4 months ago
    print(f"\n  [1b] Searching for a settled T-ticker near {TARGET_DATE}...")
    ticker_to_test: Optional[str] = None

    # Strategy 1: try /historical/markets listing endpoint
    for series_ticker, city_key, _ in KALSHI_SERIES_TO_TRY:
        print(f"       /historical/markets?series_ticker={series_ticker}...", end=" ", flush=True)
        try:
            data = await client.get("/historical/markets", params={
                "series_ticker": series_ticker,
                "limit": 200,
            })
            markets = data.get("markets", [])
            print(f"{len(markets)} markets returned")
            found = _find_near_date(markets, TARGET_DATE, days_window=60)
            if found:
                ticker_to_test = found["ticker"]
                print(f"       -> Found: {ticker_to_test}")
                break
            # paginate
            cursor = data.get("cursor")
            page = 1
            while cursor and not ticker_to_test and page < 10:
                data = await client.get("/historical/markets", params={
                    "series_ticker": series_ticker,
                    "limit": 200,
                    "cursor": cursor,
                })
                markets = data.get("markets", [])
                found = _find_near_date(markets, TARGET_DATE, days_window=60)
                if found:
                    ticker_to_test = found["ticker"]
                    print(f"       -> Found (page {page+1}): {ticker_to_test}")
                    break
                cursor = data.get("cursor")
                page += 1
                if not markets:
                    break
            if ticker_to_test:
                break
        except Exception as e:
            print(f"ERROR: {e}")

    # Strategy 2: try regular /markets without status filter
    if not ticker_to_test:
        for series_ticker, city_key, _ in KALSHI_SERIES_TO_TRY:
            print(f"       /markets?series_ticker={series_ticker} (no status)...", end=" ", flush=True)
            try:
                data = await client.get("/markets", params={
                    "series_ticker": series_ticker,
                    "limit": 200,
                })
                markets = data.get("markets", [])
                print(f"{len(markets)} markets returned")
                found = _find_near_date(markets, TARGET_DATE, days_window=60)
                if found:
                    ticker_to_test = found["ticker"]
                    print(f"       -> Found: {ticker_to_test}")
                    break
            except Exception as e:
                print(f"ERROR: {e}")

    # Strategy 3: guess tickers by format
    if not ticker_to_test:
        guesses = _generate_ticker_guesses(TARGET_DATE)
        print(f"       No listing hit. Trying {len(guesses)} guessed tickers via /historical/markets/{{t}}...")
        for guess in guesses:
            try:
                data = await client.get(f"/historical/markets/{guess}")
                mkt = data.get("market") or data
                if mkt.get("ticker") or mkt.get("result") or mkt.get("status"):
                    ticker_to_test = guess
                    print(f"       -> Guessed: {ticker_to_test}")
                    break
            except Exception:
                pass
        if not ticker_to_test:
            print("       WARNING: could not find any settled ticker near TARGET_DATE.")

    results["ticker_found"] = ticker_to_test

    # 1c. Fetch via /historical/markets/{ticker}
    if ticker_to_test:
        print(f"\n  [1c] GET /historical/markets/{ticker_to_test}...")
        try:
            hist = await client.get(f"/historical/markets/{ticker_to_test}")
            mkt = hist.get("market") or hist
            result_field = mkt.get("result")
            results["result_field"] = result_field

            meta = {}
            for key in ("result", "close_time", "expiration_time",
                        "last_price", "last_price_dollars", "volume",
                        "open_interest", "status"):
                if key in mkt:
                    meta[key] = mkt[key]
            results["settlement_metadata"] = meta

            if result_field in ("yes", "no"):
                print(f"       OK  result = '{result_field}'  (authoritative settlement)")
            else:
                print(f"       WARNING: result = {result_field!r}  (expected yes/no)")
                print(f"       All keys: {list(mkt.keys())[:20]}")
        except Exception as e:
            err = f"/historical/markets/{ticker_to_test} failed: {e}"
            results["errors"].append(err)
            print(f"       ERROR: {err}")
            # Fallback: regular /markets/{ticker}
            try:
                data = await client.get(f"/markets/{ticker_to_test}")
                mkt = data.get("market") or data
                if mkt.get("result"):
                    results["result_field"] = mkt["result"]
                    results["settlement_metadata"]["via"] = "regular_endpoint"
                    print(f"       Fallback /markets/{ticker_to_test}: result='{mkt['result']}'")
            except Exception as e2:
                print(f"       Fallback also failed: {e2}")

    # 1d. Candlestick availability
    if ticker_to_test:
        print(f"\n  [1d] Candlesticks near hypothetical scan moment (T-2 @ noon ET)...")
        ET_OFFSET = timedelta(hours=5)  # December = EST
        scan_et  = datetime(TARGET_DATE.year, TARGET_DATE.month, TARGET_DATE.day,
                            12, 0, 0) - timedelta(days=2)
        scan_utc = scan_et + ET_OFFSET
        start_ts = int(scan_utc.timestamp())
        end_ts   = start_ts + 7200  # 2-hour window

        try:
            candle_data = await client.get(
                f"/historical/markets/{ticker_to_test}/candlesticks",
                params={"start_ts": start_ts, "end_ts": end_ts, "period_interval": 60},
            )
            candles = candle_data.get("candlesticks") or candle_data.get("candles", [])
            results["candlesticks_available"] = len(candles) > 0
            if candles:
                best = candles[0]
                ask = best.get("yes_ask") or best.get("ask")
                if ask is not None:
                    ask = float(ask) / (100.0 if float(ask) > 1 else 1.0)
                results["candlestick_ask"] = ask
                print(f"       OK  {len(candles)} candle(s) at scan window, ask={ask}")
            else:
                print("       WARNING: no candles in 2-hour scan window")
                print(f"       All keys: {list(candle_data.keys())}")
        except Exception as e:
            err = f"Candlestick fetch failed: {e}"
            results["errors"].append(err)
            print(f"       ERROR: {err}")

    return results


def _find_near_date(markets: List[dict], target: date, days_window: int = 60) -> Optional[dict]:
    lo = target - timedelta(days=days_window)
    hi = target + timedelta(days=days_window)
    for m in markets:
        if re.search(r"-B\d", m.get("ticker", "")):
            continue
        ct = m.get("close_time") or m.get("expiration_time", "")
        if not ct:
            continue
        try:
            cd = datetime.fromisoformat(ct.replace("Z", "+00:00")).date()
            if lo <= cd <= hi:
                return m
        except Exception:
            pass
    return None


def _generate_ticker_guesses(d: date) -> List[str]:
    yy  = str(d.year % 100).zfill(2)
    mon = d.strftime("%b").upper()
    dd  = str(d.day).zfill(2)
    tickers = []
    for series in ["KXHIGHNY", "KXHIGHCHI", "KXHIGHDEN"]:
        for thr in [40, 42, 44, 45, 46, 48, 50, 52, 55, 35, 38, 60]:
            tickers.append(f"{series}-{yy}{mon}{dd}-T{thr}")
    return tickers


# =====================================================================
# SECTION 2 + 3: Open-Meteo Previous Runs API + Ensemble Member Check
# =====================================================================

async def check_previous_runs_api(http: httpx.AsyncClient) -> Dict[str, Any]:
    results: Dict[str, Any] = {
        "models": {},
        "ensemble_members_available": None,
        "member_test_details": {},
        "errors": [],
    }

    # The Previous Runs API exposes temperature_2m_previous_dayN as HOURLY
    # variables (not daily max/min). We request all 24 hours of TARGET_DATE
    # and compute max/min ourselves.

    models_to_test = [
        ("gfs_seamless",  "GFS",   "April 2021+"),
        ("ecmwf_ifs",     "ECMWF", "January 2024+"),
        ("gem_seamless",  "GEM",   "January 2024+"),
    ]

    for model_id, label, coverage in models_to_test:
        print(f"\n  Testing {label} ({model_id}) -- archive: {coverage}")
        model_result: Dict[str, Any] = {
            "lead_1": None, "lead_2": None, "lead_3": None,
            "raw_hourly_keys": [], "error": None,
        }

        hourly_vars = ",".join([
            "temperature_2m_previous_day1",
            "temperature_2m_previous_day2",
            "temperature_2m_previous_day3",
        ])

        params = {
            "latitude":         NYC["lat"],
            "longitude":        NYC["lon"],
            "hourly":           hourly_vars,
            "temperature_unit": "fahrenheit",
            "start_date":       TARGET_DATE.isoformat(),
            "end_date":         TARGET_DATE.isoformat(),
            "models":           model_id,
        }

        try:
            resp = await http.get(PREV_RUNS_URL, params=params, timeout=25.0)
            if resp.status_code != 200:
                model_result["error"] = f"HTTP {resp.status_code}: {resp.text[:200]}"
                print(f"    HTTP {resp.status_code}: {resp.text[:200]}")
                results["models"][model_id] = model_result
                results["errors"].append(f"{label}: {model_result['error']}")
                continue
            data = resp.json()
        except Exception as e:
            model_result["error"] = str(e)
            results["errors"].append(f"{label} fetch: {e}")
            print(f"    ERROR: {e}")
            results["models"][model_id] = model_result
            continue

        hourly = data.get("hourly", {})
        model_result["raw_hourly_keys"] = list(hourly.keys())

        for lead in (1, 2, 3):
            key = f"temperature_2m_previous_day{lead}"
            vals = hourly.get(key, [])
            vals_f = [float(v) for v in vals if v is not None]
            if vals_f:
                model_result[f"lead_{lead}"] = {
                    "max_f": round(max(vals_f), 1),
                    "min_f": round(min(vals_f), 1),
                    "n_hours": len(vals_f),
                }

        for lead in (1, 2, 3):
            v = model_result.get(f"lead_{lead}")
            if v:
                print(f"    Lead-{lead}: max={v['max_f']}F  min={v['min_f']}F  ({v['n_hours']} hourly values)  OK")
            else:
                print(f"    Lead-{lead}: NO DATA")

        results["models"][model_id] = model_result

    # ----------------------------------------------------------------
    # Section 3: Ensemble member availability (THE CRITICAL QUESTION)
    # ----------------------------------------------------------------
    print("\n  [CRITICAL] Probing for per-member ensemble data...")
    member_tests: Dict[str, Any] = {}
    any_members = False

    # Test A: Do the hourly responses above contain any member-keyed fields?
    print("    [A] Checking raw hourly keys from GFS Previous Runs response...")
    gfs_keys = results["models"].get("gfs_seamless", {}).get("raw_hourly_keys", [])
    gfs_member_keys = [k for k in gfs_keys if re.search(r"member\d+", k)]
    if gfs_member_keys:
        any_members = True
        print(f"       FOUND member keys in GFS hourly response: {gfs_member_keys[:4]}")
        member_tests["gfs_hourly_member_keys"] = gfs_member_keys[:6]
    else:
        print(f"       No member keys. All GFS hourly keys: {gfs_keys[:8]}")

    # Test B: Explicitly request a member variable on the Previous Runs API
    print("    [B] Requesting temperature_2m_previous_day2_member01 explicitly...")
    try:
        resp = await http.get(
            PREV_RUNS_URL,
            params={
                "latitude":         NYC["lat"],
                "longitude":        NYC["lon"],
                "hourly":           "temperature_2m_previous_day2_member01",
                "temperature_unit": "fahrenheit",
                "start_date":       TARGET_DATE.isoformat(),
                "end_date":         TARGET_DATE.isoformat(),
                "models":           "gfs_seamless",
            },
            timeout=25.0,
        )
        b = {
            "status": resp.status_code,
            "has_member_key": False,
        }
        if resp.status_code == 200:
            hourly = resp.json().get("hourly", {})
            keys = list(hourly.keys())
            member_keys = [k for k in keys if "member" in k]
            b["has_member_key"] = bool(member_keys)
            b["keys"] = keys
            if member_keys:
                any_members = True
                print(f"       SUCCESS: member variable accepted. Keys: {keys[:5]}")
            else:
                print(f"       HTTP 200 but no member key. Keys: {keys}")
        else:
            b["body"] = resp.text[:300]
            print(f"       HTTP {resp.status_code}: {resp.text[:250]}")
        member_tests["explicit_member_prev_runs"] = b
    except Exception as e:
        member_tests["explicit_member_prev_runs"] = {"error": str(e)}
        print(f"       ERROR: {e}")

    # Test C: Can the Ensemble API serve historical dates with per-member data?
    # We request the date 2 days BEFORE TARGET_DATE (the scan date).
    # If this works, we'd get ensemble members -- but IMPORTANT: this would
    # be the ensemble's CURRENT-run hindcast, not the T-2 forecast from Dec 13.
    scan_date = TARGET_DATE - timedelta(days=2)
    print(f"    [C] Ensemble API with scan_date={scan_date} (T-2, looking for hindcast)...")
    try:
        resp = await http.get(
            ENSEMBLE_URL,
            params={
                "latitude":         NYC["lat"],
                "longitude":        NYC["lon"],
                "daily":            "temperature_2m_max",
                "temperature_unit": "fahrenheit",
                "start_date":       scan_date.isoformat(),
                "end_date":         TARGET_DATE.isoformat(),
                "models":           "gfs_seamless",
            },
            timeout=25.0,
        )
        c = {"status": resp.status_code}
        if resp.status_code == 200:
            daily = resp.json().get("daily", {})
            keys = list(daily.keys())
            member_keys = [k for k in keys if re.search(r"member\d+", k)]
            c["num_member_keys"] = len(member_keys)
            c["sample_keys"] = member_keys[:4]
            c["total_keys"] = len(keys)
            if member_keys:
                print(f"       Ensemble API HAS {len(member_keys)} member keys for {scan_date}")
                print(f"       e.g. {member_keys[:3]}")
                print("       NOTE: this is the CURRENT model run's hindcast, not the")
                print("       Dec 13 forecast. Cannot use for honest backtesting.")
            else:
                print(f"       No member keys. Total daily keys: {len(keys)}. Sample: {keys[:6]}")
        else:
            c["body"] = resp.text[:300]
            print(f"       HTTP {resp.status_code}: {resp.text[:200]}")
        member_tests["ensemble_api_hindcast"] = c
    except Exception as e:
        member_tests["ensemble_api_hindcast"] = {"error": str(e)}
        print(f"       ERROR: {e}")

    # Test D: Previous Runs API, hourly, requesting many member variables at once
    # Some models expose members as separate keys when you ask for enough vars.
    print("    [D] Previous Runs hourly -- requesting all 31 GFS member vars...")
    member_vars = ",".join([f"temperature_2m_previous_day2_member{str(i).zfill(2)}" for i in range(1, 32)])
    try:
        resp = await http.get(
            PREV_RUNS_URL,
            params={
                "latitude":         NYC["lat"],
                "longitude":        NYC["lon"],
                "hourly":           member_vars,
                "temperature_unit": "fahrenheit",
                "start_date":       TARGET_DATE.isoformat(),
                "end_date":         TARGET_DATE.isoformat(),
                "models":           "gfs_seamless",
            },
            timeout=25.0,
        )
        d = {"status": resp.status_code}
        if resp.status_code == 200:
            hourly = resp.json().get("hourly", {})
            keys = list(hourly.keys())
            member_keys = [k for k in keys if "member" in k]
            d["num_member_keys"] = len(member_keys)
            d["sample_keys"] = member_keys[:4]
            if member_keys:
                any_members = True
                print(f"       SUCCESS: {len(member_keys)} member keys returned!")
                print(f"       Sample: {member_keys[:4]}")
            else:
                print(f"       HTTP 200 but no member keys. Keys: {keys[:6]}")
        else:
            d["body"] = resp.text[:300]
            print(f"       HTTP {resp.status_code}: {resp.text[:250]}")
        member_tests["prev_runs_all_gfs_members"] = d
    except Exception as e:
        member_tests["prev_runs_all_gfs_members"] = {"error": str(e)}
        print(f"       ERROR: {e}")

    results["ensemble_members_available"] = any_members
    results["member_test_details"] = member_tests
    return results


# =====================================================================
# SECTION 4: Code health check
# =====================================================================

def check_market_direction_bug() -> Dict[str, Any]:
    try:
        from backend.data.weather_markets import WeatherMarket
        import dataclasses
        fields = {f.name for f in dataclasses.fields(WeatherMarket)}
        has_field = "market_direction" in fields
        return {
            "market_direction_field_exists": has_field,
            "fields": sorted(fields),
            "diagnosis": (
                "OK -- no bug"
                if has_field else
                "BUG: weather_signals.py L371 accesses market.market_direction but "
                "WeatherMarket only has .direction -- AttributeError on every signal. "
                "Likely introduced in 'Add cold-day exception to 30c YES floor' commit. "
                "Fix for backtest: set market.market_direction = market.direction "
                "after constructing each WeatherMarket."
            ),
        }
    except Exception as e:
        return {"error": str(e)}


# =====================================================================
# Main
# =====================================================================

async def main() -> None:
    print("=" * 70)
    print("  Phase 1: Backtest Data Verification")
    print(f"  Target date tested: {TARGET_DATE}")
    print(f"  Today:              {date.today()}")
    print("=" * 70)

    print("\n[PRE-FLIGHT] Kalshi credentials...")
    kalshi_ok = kalshi_credentials_present()
    print(f"  {'OK  credentials present' if kalshi_ok else 'SKIP  credentials missing'}")

    kalshi_results: Dict[str, Any] = {}
    if kalshi_ok:
        print("\n" + "-" * 70)
        print("SECTION 1: Kalshi Historical Market API")
        print("-" * 70)
        kalshi_results = await check_kalshi_historical(KalshiClient())
    else:
        print("  Skipping Kalshi section.")

    print("\n" + "-" * 70)
    print("SECTION 2+3: Open-Meteo Previous Runs + Ensemble Member Check")
    print("-" * 70)
    openmeteo_results: Dict[str, Any] = {}
    async with httpx.AsyncClient() as http:
        openmeteo_results = await check_previous_runs_api(http)

    print("\n" + "-" * 70)
    print("SECTION 4: Code Health")
    print("-" * 70)
    bug_report = check_market_direction_bug()

    # ---- Summary ----
    print("\n" + "=" * 70)
    print("  VERIFICATION SUMMARY")
    print("=" * 70)

    print(f"\n1. Kalshi Historical API")
    print(f"   Cutoff date:    {kalshi_results.get('historical_cutoff', 'N/A')}")
    print(f"   Ticker tested:  {kalshi_results.get('ticker_found') or 'NONE FOUND'}")
    rf = kalshi_results.get("result_field")
    if rf in ("yes", "no"):
        print(f"   Result field:   OK '{rf}'  (authoritative settlement)")
    elif rf is not None:
        print(f"   Result field:   WARNING '{rf}'")
    else:
        print(f"   Result field:   NOT FOUND")
    ca = kalshi_results.get("candlesticks_available")
    ck = kalshi_results.get("candlestick_ask")
    if ca is True:
        print(f"   Candle ask:     OK {ck}")
    elif ca is False:
        print(f"   Candle ask:     NO candles at T-2 noon scan window")
    else:
        print(f"   Candle ask:     not tested (no ticker)")
    for e in kalshi_results.get("errors", []):
        print(f"   ! {e}")

    print(f"\n2. Open-Meteo Previous Runs API  (target={TARGET_DATE})")
    for model_id, label, _ in [
        ("gfs_seamless", "GFS",   ""),
        ("ecmwf_ifs",    "ECMWF", ""),
        ("gem_seamless", "GEM",   ""),
    ]:
        m = openmeteo_results.get("models", {}).get(model_id, {})
        if m.get("error"):
            print(f"   {label:6s}: ERROR  {m['error'][:70]}")
        else:
            vals = []
            for lead in (1, 2, 3):
                v = m.get(f"lead_{lead}")
                vals.append(f"T-{lead}={v['max_f']}F" if v else f"T-{lead}=MISSING")
            ok = all(m.get(f"lead_{lead}") for lead in (1, 2, 3))
            print(f"   {label:6s}: {'OK' if ok else 'PARTIAL'}  {', '.join(vals)}")

    print(f"\n3. CRITICAL -- Per-member ensemble data availability")
    members = openmeteo_results.get("ensemble_members_available")
    if members is True:
        print(f"   OK  Per-member data IS available historically.")
        print(f"   -> Full ensemble_fraction model can be replicated in the backtest.")
    else:
        print(f"   BLOCKED  Per-member data is NOT available historically.")
        print(f"   -> Previous Runs API gives one forecast value per model run")
        print(f"      (the ensemble mean), not individual member temperatures.")
        print(f"   -> The live model: 70% ensemble_fraction + 30% Gaussian CDF")
        print(f"      ensemble_fraction = fraction of members above/below threshold")
        print(f"      This CANNOT be reconstructed without individual member values.")
        print(f"")
        print(f"   OPTIONS -- discuss before proceeding to Phase 2:")
        print(f"   a) Use Gaussian CDF only (approximates what the model would output")
        print(f"      when the ensemble is tight). Covers 30% of the live signal.")
        print(f"      Caveat: model_prob may differ from live bot's by 5-15pp.")
        print(f"   b) Use ensemble mean + std floor (3F for highs, 2F for lows)")
        print(f"      to approximate ensemble_fraction via Gaussian CDF -- same")
        print(f"      limitation but makes the approximation explicit.")
        print(f"   c) Skip model replication entirely; audit the MARKET PRICE only:")
        print(f"      measure whether market prices were well-calibrated against")
        print(f"      actual outcomes (Brier score on market probability, not model).")
        print(f"   d) Audit live-era signal logs vs outcomes (test only the 2-week")
        print(f"      live record; no historical extension needed).")

    print(f"\n4. Code Health")
    if bug_report.get("market_direction_field_exists"):
        print(f"   OK  WeatherMarket.market_direction field exists")
    else:
        print(f"   BUG  {bug_report.get('diagnosis', '')[:180]}")

    print(f"\n{'=' * 70}")
    out_path = "/tmp/verify_backtest_phase1.json"
    with open(out_path, "w") as f:
        json.dump({
            "date_checked": date.today().isoformat(),
            "target_date":  TARGET_DATE.isoformat(),
            "kalshi":       kalshi_results,
            "open_meteo":   openmeteo_results,
            "code_health":  bug_report,
        }, f, indent=2, default=str)
    print(f"  Full JSON saved to: {out_path}")
    print("=" * 70)


if __name__ == "__main__":
    asyncio.run(main())
