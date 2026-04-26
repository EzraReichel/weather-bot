#!/usr/bin/env python3
"""
Reconstruct paper trade P&L from signals in weatherbot.db.
Skips bracket (B-ticker) markets and early signals with fake 0.50 prices.
Fetches actual NWS temps and calculates W/L/P&L.
"""
from dotenv import load_dotenv
load_dotenv()

import asyncio
import json
import sqlite3
from datetime import date, datetime, timedelta
from collections import defaultdict

FEE_RATE = 0.07
KELLY_FRACTION = 0.15
BANKROLL = 1000.0
MAX_TRADE = 100.0
MIN_EDGE = 0.08

# ── Load signals ──────────────────────────────────────────────────────────────
conn = sqlite3.connect("weatherbot.db")
cur = conn.cursor()
cur.execute("""
    SELECT market_ticker, direction, model_probability, market_price,
           edge, suggested_size, timestamp, sources
    FROM signals
    WHERE market_type = 'weather'
      AND ABS(edge) >= ?
      AND market_price != 0.5
      AND sources LIKE '%agreement%'
    ORDER BY timestamp ASC
""", (MIN_EDGE,))
rows = cur.fetchall()
conn.close()

print(f"Raw signals above threshold (excluding fake 0.50 prices): {len(rows)}")

# ── Parse ticker ───────────────────────────────────────────────────────────────
import re

CITY_MAP = {
    "NY": "nyc", "CHI": "chicago", "MIA": "miami", "LAX": "los_angeles",
    "DEN": "denver", "AUS": "austin", "HOU": "houston", "BOS": "boston",
    "DC": "washington_dc", "PHX": "phoenix", "SEA": "seattle", "SF": "san_francisco",
    "ATL": "atlanta", "DAL": "dallas", "LV": "las_vegas", "MIN": "minneapolis",
    "NO": "new_orleans", "OKC": "oklahoma_city", "SA": "san_antonio", "PHL": "philadelphia",
    "TCHI": "chicago",  # KXLOWTCHI etc.
    "TLAX": "los_angeles", "TDEN": "denver", "TMIA": "miami",
}

def parse_ticker(ticker):
    """Returns (city_key, metric, direction, threshold_f, target_date) or None."""
    # KXHIGHNY-26APR09-T53  or  KXLOWTDEN-26APR09-T38
    # Format: KXHIGHNY-26APR09-T53  →  year=26, month=APR, day=09
    m = re.match(r'KX(HIGH|LOW)([A-Z]+)-(\d{2})([A-Z]{3})(\d{2})-T(\d+)', ticker)
    if not m:
        return None
    metric_raw, city_raw, yr, mon_str, day, thresh = m.groups()
    metric = "high" if metric_raw == "HIGH" else "low"

    # city_raw might be "NY", "CHI", "TCHI" etc.
    city_key = CITY_MAP.get(city_raw)
    if not city_key:
        # try stripping leading T
        city_key = CITY_MAP.get(city_raw.lstrip("T"))
    if not city_key:
        return None

    months = {"JAN":1,"FEB":2,"MAR":3,"APR":4,"MAY":5,"JUN":6,
              "JUL":7,"AUG":8,"SEP":9,"OCT":10,"NOV":11,"DEC":12}
    month = months.get(mon_str.upper())
    if not month:
        return None

    target_date = date(2000 + int(yr), month, int(day))
    threshold_f = float(thresh)

    # T-ticker: YES = temp BELOW threshold for HIGH markets, ABOVE for LOW?
    # Actually for Kalshi KXHIGH T-tickers: YES = high temp < threshold
    # For KXLOW T-tickers: YES = low temp > threshold
    # direction here = what YES resolves as
    # "above" means YES wins if temp > threshold, "below" means YES wins if temp < threshold
    if metric == "high":
        direction = "below"  # "Will the HIGH be BELOW X?" = YES condition
    else:
        direction = "above"  # "Will the LOW be ABOVE X?" = YES condition

    return city_key, metric, direction, threshold_f, target_date


# ── Deduplicate: first signal per ticker ──────────────────────────────────────
seen = {}
signals = []
for row in rows:
    ticker, direction, model_prob, market_price, edge, suggested_size, ts, sources_json = row
    if ticker in seen:
        continue
    # Skip bracket markets
    if re.search(r'-B\d', ticker):
        continue
    parsed = parse_ticker(ticker)
    if parsed is None:
        print(f"  SKIP (unparseable): {ticker}")
        continue
    city_key, metric, mkt_direction, threshold_f, target_date = parsed

    # Skip if resolution date is today or in the future
    if target_date >= date.today():
        continue

    seen[ticker] = True
    entry_price = market_price if direction == "yes" else (1.0 - market_price)
    # Kelly sizing
    if direction == "yes":
        p = model_prob
        b_net = ((1.0 - entry_price) / entry_price) * (1.0 - FEE_RATE)
        q = 1.0 - p
    else:
        p = 1.0 - model_prob
        b_net = ((1.0 - entry_price) / entry_price) * (1.0 - FEE_RATE)
        q = 1.0 - p

    kelly_f = (b_net * p - q) / b_net if b_net > 0 else 0
    kelly_f = max(0, min(kelly_f, 1.0))
    size = round(min(BANKROLL * KELLY_FRACTION * kelly_f, MAX_TRADE), 2)
    if size <= 0:
        size = min(10.0, MAX_TRADE)  # floor bet
    contracts = max(1, int(size / entry_price))

    try:
        src = json.loads(sources_json) if isinstance(sources_json, str) else sources_json
        agreement = src.get("agreement", "UNKNOWN") if isinstance(src, dict) else "UNKNOWN"
        source_probs = src.get("source_probs", {}) if isinstance(src, dict) else {}
    except Exception:
        agreement = "UNKNOWN"
        source_probs = {}

    signals.append({
        "ticker": ticker,
        "city_key": city_key,
        "metric": metric,
        "direction": direction,         # which side we bet
        "mkt_direction": mkt_direction, # YES condition ("above"/"below")
        "threshold_f": threshold_f,
        "target_date": target_date,
        "model_prob": model_prob,
        "market_price": market_price,
        "edge": edge,
        "entry_price": entry_price,
        "contracts": contracts,
        "size": size,
        "agreement": agreement,
        "source_probs": source_probs,
        "timestamp": ts,
    })

print(f"Unique settled T-ticker signals: {len(signals)}")


# ── Fetch NWS actuals ─────────────────────────────────────────────────────────
async def fetch_all_actuals(signals):
    from backend.data.weather import fetch_nws_observed_temperature

    # Batch by (city, date) to avoid redundant calls
    needed = {}
    for s in signals:
        key = (s["city_key"], s["target_date"])
        needed[key] = None

    print(f"\nFetching {len(needed)} city/date combos from NWS...")
    for (city_key, target_date) in needed:
        result = await fetch_nws_observed_temperature(city_key, target_date)
        needed[(city_key, target_date)] = result
        status = f"high={result['high']:.1f}°F  low={result['low']:.1f}°F" if result else "NO DATA"
        print(f"  {city_key:15s} {target_date}  {status}")

    return needed

actuals = asyncio.run(fetch_all_actuals(signals))


# ── Settle and calculate P&L ──────────────────────────────────────────────────
print("\n" + "="*70)
print("  RECONSTRUCTED PAPER TRADE RESULTS")
print("="*70)
print(f"  {'Ticker':<42} {'Side':<4} {'Edge':>6}  {'Actual':>7}  {'Thresh':>6}  {'Result':<6}  {'P&L':>8}")
print("-"*70)

total_pnl = 0.0
wins = losses = no_data = 0
city_stats = defaultdict(lambda: {"wins": 0, "losses": 0, "pnl": 0.0})
agreement_stats = defaultdict(lambda: {"wins": 0, "losses": 0, "pnl": 0.0})

for s in sorted(signals, key=lambda x: x["target_date"]):
    obs = actuals.get((s["city_key"], s["target_date"]))
    if obs is None:
        print(f"  {s['ticker']:<42} {'?':4}  {s['edge']:>+6.0%}  {'NO DATA':>7}  {s['threshold_f']:>6.0f}°F  {'n/a':<6}  {'':>8}")
        no_data += 1
        continue

    actual_temp = obs.get(s["metric"])
    if actual_temp is None:
        no_data += 1
        continue

    # Did YES win?
    if s["mkt_direction"] == "below":
        yes_wins = actual_temp < s["threshold_f"]
    else:
        yes_wins = actual_temp > s["threshold_f"]

    we_win = yes_wins if s["direction"] == "yes" else not yes_wins

    if we_win:
        pnl = (1.0 - s["entry_price"]) * s["contracts"] * (1.0 - FEE_RATE)
        result = "WIN"
        wins += 1
    else:
        pnl = s["entry_price"] * s["contracts"] * -1.0
        result = "LOSS"
        losses += 1

    pnl = round(pnl, 2)
    total_pnl += pnl

    city_stats[s["city_key"]]["wins" if we_win else "losses"] += 1
    city_stats[s["city_key"]]["pnl"] += pnl
    agreement_stats[s["agreement"]]["wins" if we_win else "losses"] += 1
    agreement_stats[s["agreement"]]["pnl"] += pnl

    icon = "✅" if we_win else "❌"
    print(f"  {s['ticker']:<42} {s['direction'].upper():<4}  {s['edge']:>+6.0%}  "
          f"{actual_temp:>6.1f}°F  {s['threshold_f']:>6.0f}°F  "
          f"{icon} {result:<4}  ${pnl:>+7.2f}")

print("="*70)
total = wins + losses
win_rate = wins / total * 100 if total > 0 else 0
sign = "+" if total_pnl >= 0 else ""
print(f"\n  TOTAL:  {total} trades  |  {wins}W / {losses}L  ({win_rate:.0f}% win rate)")
print(f"  P&L:    {sign}${total_pnl:.2f}")
if no_data:
    print(f"  Skipped (no NWS data): {no_data}")

print(f"\n  BY CITY:")
for city, s in sorted(city_stats.items(), key=lambda x: -x[1]["pnl"]):
    t = s["wins"] + s["losses"]
    sign = "+" if s["pnl"] >= 0 else ""
    print(f"    {city:<20}  {s['wins']}W/{s['losses']}L  {sign}${s['pnl']:.2f}")

print(f"\n  BY AGREEMENT:")
for lvl in ["HIGH", "MEDIUM", "LOW", "UNKNOWN"]:
    if lvl in agreement_stats:
        s = agreement_stats[lvl]
        t = s["wins"] + s["losses"]
        wr = s["wins"] / t * 100 if t > 0 else 0
        sign = "+" if s["pnl"] >= 0 else ""
        print(f"    {lvl:<10}  {s['wins']}W/{s['losses']}L  ({wr:.0f}%)  {sign}${s['pnl']:.2f}")
