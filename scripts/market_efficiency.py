#!/usr/bin/env python3
"""
scripts/market_efficiency.py -- Historical market-efficiency audit.

For each settled Kalshi weather T-ticker in our cities, fetches the market's
entry price (yes_ask at first traded candle after opening), the authoritative
result, and the actual observed temperature (expiration_value). Builds a
calibration curve and Brier score to answer:

  "Are Kalshi weather markets well-calibrated, or is there systematic
   mispricing that a model with accurate forecasts could exploit?"

Key design decision: These markets open 1-2 days before settlement and have no
"T-2 noon" price (the market didn't exist then). We use the yes_ask.close from
the FIRST candle with real trading after market open. This is the earliest price
a live bot could realistically enter at.

Outputs:
  backtest_results/market_efficiency_trades.csv
  backtest_results/market_efficiency_summary.json
  Calibration summary printed to stdout.

Usage:
  source .venv/bin/activate
  python scripts/market_efficiency.py [--series KXHIGHNY,KXHIGHCHI,...] [--max-pages 15]
"""
import argparse
import asyncio
import csv
import hashlib
import json
import math
import random
import re
import sys
import time
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from dotenv import load_dotenv

load_dotenv()
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from backend.data.kalshi_client import KalshiClient
from backend.data.kalshi_markets import KNOWN_SERIES_MAP, MONTH_ABBR, _parse_temp_ticker

CACHE_DIR = Path("cache/backtest/efficiency")
OUT_DIR   = Path("backtest_results")

RATE_LIMIT_SLEEP = 0.25  # seconds between Kalshi requests

# All series we care about (bot cities, T-tickers only)
DEFAULT_SERIES = [
    "KXHIGHNY",  "KXLOWNY",
    "KXHIGHCHI", "KXLOWCHI",
    "KXHIGHMIA", "KXLOWMIA",
    "KXHIGHDEN", "KXLOWDEN",
    "KXHIGHLAX", "KXLOWLAX",
]


# =====================================================================
# Disk cache helpers
# =====================================================================

def _cache_key(url: str, params: dict) -> str:
    raw = url + json.dumps(params, sort_keys=True)
    return hashlib.md5(raw.encode()).hexdigest()


def _read_cache(key: str) -> Optional[dict]:
    path = CACHE_DIR / f"{key}.json"
    if path.exists():
        try:
            return json.loads(path.read_text())
        except Exception:
            pass
    return None


def _write_cache(key: str, data: dict) -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    (CACHE_DIR / f"{key}.json").write_text(json.dumps(data))


async def _cached_get(client: KalshiClient, path: str, params: dict = {}) -> dict:
    key = _cache_key(path, params)
    cached = _read_cache(key)
    if cached is not None:
        return cached
    await asyncio.sleep(RATE_LIMIT_SLEEP)
    data = await client.get(path, params or None)
    _write_cache(key, data)
    return data


# =====================================================================
# Step 1: Fetch all historical settled T-ticker markets
# =====================================================================

async def fetch_series_markets(
    client: KalshiClient,
    series_ticker: str,
    max_pages: int,
) -> List[dict]:
    """
    Page through /historical/markets for a given series.
    Returns only T-ticker (non-bracket) markets that have a valid result.
    """
    city_key, metric = KNOWN_SERIES_MAP.get(series_ticker, (None, None))
    if not city_key:
        return []

    markets: List[dict] = []
    cursor: Optional[str] = None
    page = 0

    while page < max_pages:
        params: Dict[str, Any] = {"series_ticker": series_ticker, "limit": 200}
        if cursor:
            params["cursor"] = cursor

        try:
            data = await _cached_get(client, "/historical/markets", params)
        except Exception as e:
            print(f"    WARNING: {series_ticker} page {page}: {e}")
            break

        raw = data.get("markets", [])
        for m in raw:
            ticker = m.get("ticker", "")
            if not ticker or re.search(r"-B\d", ticker):
                continue
            result = m.get("result", "")
            if result not in ("yes", "no"):
                continue
            title = m.get("title", ticker)
            parsed = _parse_temp_ticker(ticker, city_key, metric, title)
            if not parsed:
                continue
            m["_city_key"] = city_key
            m["_metric"]   = metric
            m["_target_date"]  = parsed["target_date"]
            m["_threshold_f"]  = parsed["threshold_f"]
            m["_direction"]    = parsed["direction"]
            markets.append(m)

        cursor = data.get("cursor")
        page += 1
        if not cursor or not raw:
            break

    return markets


# =====================================================================
# Step 2: Get entry price from candlesticks
# =====================================================================

async def get_entry_price(
    client: KalshiClient,
    ticker: str,
    open_ts: int,
    close_ts: int,
) -> Tuple[Optional[float], str]:
    """
    Fetch candlestick data for the market's full trading window and return
    the YES ask from the first candle with actual trading activity.

    Returns (price, source) where source describes what was found.
    """
    # Widen end slightly to capture any late candles
    params = {
        "start_ts":       open_ts,
        "end_ts":         close_ts + 3600,
        "period_interval": 60,
    }
    try:
        data = await _cached_get(client, f"/historical/markets/{ticker}/candlesticks", params)
    except Exception as e:
        return None, f"candlestick_error:{e}"

    candles = data.get("candlesticks", [])
    if not candles:
        return None, "no_candles"

    # Walk through candles and find first one with real trading
    # Price priority: yes_ask.close > price.close > yes_ask.open
    for c in candles:
        vol = float(c.get("volume") or 0)
        ask_close = c.get("yes_ask", {}).get("close")
        price_close = c.get("price", {}).get("close")
        ask_open  = c.get("yes_ask", {}).get("open")

        # Try price.close first (mid of last trade in period)
        p = None
        src = "none"
        if price_close is not None:
            p = float(price_close)
            src = "price_close"
        elif ask_close is not None:
            p = float(ask_close)
            src = "ask_close"
        elif ask_open is not None and float(ask_open) < 0.999:
            p = float(ask_open)
            src = "ask_open"

        # Skip degenerate values (just-opened ask at $1 or settled at $0/$1)
        if p is not None and 0.01 < p < 0.99:
            return round(p, 4), src

    # Fallback: take the first candle's ask_close even if outside the nice range,
    # as long as it's between 0.01 and 0.99
    for c in candles:
        ask_close = c.get("yes_ask", {}).get("close")
        if ask_close is not None:
            p = float(ask_close)
            if 0.01 <= p <= 0.99:
                return round(p, 4), "ask_close_fallback"

    return None, "no_valid_price"


# =====================================================================
# Step 3: Calibration and statistics
# =====================================================================

def brier_score(pairs: List[Tuple[float, int]]) -> float:
    if not pairs:
        return float("nan")
    return sum((p - o) ** 2 for p, o in pairs) / len(pairs)


def brier_ci_bootstrap(pairs: List[Tuple[float, int]], n: int = 2000, seed: int = 42) -> Tuple[float, float]:
    """95% confidence interval via bootstrap resampling."""
    rng = random.Random(seed)
    n_obs = len(pairs)
    if n_obs < 5:
        return float("nan"), float("nan")
    scores = []
    for _ in range(n):
        sample = [rng.choice(pairs) for _ in range(n_obs)]
        scores.append(brier_score(sample))
    scores.sort()
    lo = scores[int(0.025 * n)]
    hi = scores[int(0.975 * n)]
    return lo, hi


def win_rate(pairs: List[Tuple[float, int]]) -> float:
    if not pairs:
        return float("nan")
    return sum(o for _, o in pairs) / len(pairs)


def calibration_curve(pairs: List[Tuple[float, int]], buckets: int = 10) -> List[dict]:
    """Bin by predicted probability, compute actual win rate per bin."""
    bins: Dict[int, List[Tuple[float, int]]] = defaultdict(list)
    for p, o in pairs:
        bucket = min(int(p * buckets), buckets - 1)
        bins[bucket].append((p, o))
    result = []
    for i in range(buckets):
        pts = bins[i]
        lo = i / buckets
        hi = (i + 1) / buckets
        mid = (lo + hi) / 2
        result.append({
            "bucket_lo":    round(lo, 2),
            "bucket_hi":    round(hi, 2),
            "bucket_mid":   round(mid, 2),
            "n":            len(pts),
            "mean_price":   round(sum(p for p, _ in pts) / len(pts), 4) if pts else None,
            "actual_rate":  round(sum(o for _, o in pts) / len(pts), 4) if pts else None,
            "bias_pp":      round((sum(o for _, o in pts) / len(pts) - mid) * 100, 1) if pts else None,
        })
    return result


def implied_brier(pairs: List[Tuple[float, int]]) -> float:
    """Brier score if market prices were perfectly calibrated (theoretical minimum)."""
    if not pairs:
        return float("nan")
    # For a well-calibrated market, E[BS] = mean(p*(1-p))
    return sum(p * (1 - p) for p, _ in pairs) / len(pairs)


def market_breakeven_win_rate(price: float, fee_rate: float = 0.07) -> float:
    """Fraction of the time YES must win to break even at `price` with Kalshi fees."""
    if price <= 0 or price >= 1:
        return float("nan")
    # payout per $ risked (net of fee): (1 - price) * (1 - fee_rate)
    # cost per $ risked: price
    # breakeven: price = (1-price)*(1-fee_rate)*p_win  =>  p_win = price / ((1-price)*(1-fee_rate))
    return price / ((1 - price) * (1 - fee_rate))


# =====================================================================
# Main
# =====================================================================

async def main(series_list: List[str], max_pages: int) -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    client = KalshiClient()

    print(f"\n{'='*70}")
    print("  Market Efficiency Audit")
    print(f"  Series: {', '.join(series_list)}")
    print(f"  Max pages per series: {max_pages}")
    print(f"{'='*70}\n")

    # ------------------------------------------------------------------
    # Phase A: Collect all settled markets
    # ------------------------------------------------------------------
    all_markets: List[dict] = []
    for series in series_list:
        print(f"  Fetching {series}...", end=" ", flush=True)
        mkts = await fetch_series_markets(client, series, max_pages)
        print(f"{len(mkts)} settled T-tickers")
        all_markets.extend(mkts)

    print(f"\n  Total markets collected: {len(all_markets)}")
    if not all_markets:
        print("  No markets found. Check Kalshi credentials and series names.")
        return

    # ------------------------------------------------------------------
    # Phase B: Get entry price for each market
    # ------------------------------------------------------------------
    print(f"\n  Fetching candlestick entry prices...")

    trades: List[dict] = []
    skipped_no_price = 0
    skipped_no_open  = 0
    price_sources: Dict[str, int] = defaultdict(int)

    for i, m in enumerate(all_markets):
        ticker = m["ticker"]
        open_time  = m.get("open_time", "")
        close_time = m.get("close_time", "")

        if not open_time or not close_time:
            skipped_no_open += 1
            continue

        open_ts  = int(datetime.fromisoformat(open_time.replace("Z", "+00:00")).timestamp())
        close_ts = int(datetime.fromisoformat(close_time.replace("Z", "+00:00")).timestamp())

        entry_price, src = await get_entry_price(client, ticker, open_ts, close_ts)
        price_sources[src] += 1

        result = m.get("result", "")
        outcome = 1 if result == "yes" else 0

        if entry_price is None:
            skipped_no_price += 1
            continue

        exp_val = m.get("expiration_value")
        exp_f   = float(exp_val) if exp_val else None

        trades.append({
            "ticker":        ticker,
            "city":          m.get("_city_key", ""),
            "metric":        m.get("_metric", ""),
            "target_date":   m.get("_target_date", "").isoformat() if hasattr(m.get("_target_date"), "isoformat") else str(m.get("_target_date", "")),
            "threshold_f":   m.get("_threshold_f", ""),
            "direction":     m.get("_direction", ""),
            "open_time":     open_time[:10],
            "entry_price":   entry_price,
            "price_source":  src,
            "result":        result,
            "outcome":       outcome,
            "expiry_val_f":  exp_f,
            "volume":        float(m.get("volume_fp") or 0),
            "breakeven_wr":  round(market_breakeven_win_rate(entry_price), 4),
            "actual_minus_price": round(outcome - entry_price, 4),
        })

        if (i + 1) % 100 == 0:
            pct_priced = len(trades) / (i + 1 - skipped_no_open) * 100
            print(f"    {i+1}/{len(all_markets)}  priced={len(trades)} ({pct_priced:.0f}%)")

    print(f"\n  Entry price results:")
    print(f"    Priced:           {len(trades)}")
    print(f"    Skipped/no candle:{skipped_no_price}")
    print(f"    Skipped/no open:  {skipped_no_open}")
    print(f"    Price sources:    {dict(sorted(price_sources.items()))}")

    if not trades:
        print("  No trades with entry prices. Cannot compute calibration.")
        return

    # ------------------------------------------------------------------
    # Phase C: Write CSV
    # ------------------------------------------------------------------
    csv_path = OUT_DIR / "market_efficiency_trades.csv"
    fieldnames = list(trades[0].keys())
    with open(csv_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(trades)
    print(f"\n  Trades saved to: {csv_path}")

    # ------------------------------------------------------------------
    # Phase D: Compute efficiency metrics
    # ------------------------------------------------------------------
    pairs = [(t["entry_price"], t["outcome"]) for t in trades]

    bs = brier_score(pairs)
    bs_implied = implied_brier(pairs)
    bs_ci_lo, bs_ci_hi = brier_ci_bootstrap(pairs)
    bs_random = 0.25  # always predict 50%
    wr = win_rate(pairs)
    mean_price = sum(p for p, _ in pairs) / len(pairs)

    print(f"\n{'='*70}")
    print("  MARKET EFFICIENCY SUMMARY")
    print(f"{'='*70}")

    print(f"\n  Markets analyzed:    {len(trades)}")
    print(f"  Date range:          {min(t['open_time'] for t in trades)} – {max(t['open_time'] for t in trades)}")
    print(f"  Mean entry price:    {mean_price:.3f}  (YES ask at first traded candle)")
    print(f"  Actual win rate:     {wr:.3f}  (fraction of markets that resolved YES)")
    print(f"  Mean bias (pp):      {(wr - mean_price)*100:+.1f}pp  (positive = YES wins more than priced)")

    print(f"\n  Brier Score (market):  {bs:.4f}  95%CI [{bs_ci_lo:.4f}, {bs_ci_hi:.4f}]")
    print(f"  Brier Score (implied): {bs_implied:.4f}  (theoretical if prices were perfect)")
    print(f"  Brier Score (50/50):   {bs_random:.4f}  (random baseline)")
    print(f"  Skill vs random:       {(1 - bs / bs_random)*100:.1f}%")
    print(f"  Slack (market vs implied): {(bs - bs_implied)*100:.2f}pp Brier  (reducible error)")

    # Calibration curve
    curve = calibration_curve(pairs, buckets=10)
    print(f"\n  Calibration Curve (market price bucket → actual win rate):")
    print(f"  {'Bucket':10s} {'N':>6} {'Mean Price':>11} {'Actual Rate':>12} {'Bias (pp)':>10}")
    print(f"  {'-'*55}")
    for row in curve:
        if row["n"] == 0:
            continue
        bias_str = f"{row['bias_pp']:+.1f}" if row["bias_pp"] is not None else "N/A"
        print(f"  {row['bucket_lo']:.0%}–{row['bucket_hi']:.0%}    {row['n']:>6} {row['mean_price']:>11.3f} {row['actual_rate']:>12.3f} {bias_str:>10}")

    # By city
    print(f"\n  By City:")
    print(f"  {'City':12s} {'N':>5} {'Mean P':>8} {'Win%':>7} {'Brier':>8} {'Bias (pp)':>10}")
    print(f"  {'-'*55}")
    city_groups: Dict[str, list] = defaultdict(list)
    for t in trades:
        city_groups[t["city"]].append((t["entry_price"], t["outcome"]))
    for city, cp in sorted(city_groups.items(), key=lambda x: -len(x[1])):
        mp = sum(p for p, _ in cp) / len(cp)
        wr_c = win_rate(cp)
        bs_c = brier_score(cp)
        bias = (wr_c - mp) * 100
        print(f"  {city:12s} {len(cp):>5} {mp:>8.3f} {wr_c:>7.3f} {bs_c:>8.4f} {bias:>+10.1f}pp")

    # By metric (high/low)
    print(f"\n  By Metric:")
    metric_groups: Dict[str, list] = defaultdict(list)
    for t in trades:
        metric_groups[t["metric"]].append((t["entry_price"], t["outcome"]))
    for metric, cp in sorted(metric_groups.items()):
        mp = sum(p for p, _ in cp) / len(cp)
        wr_c = win_rate(cp)
        bs_c = brier_score(cp)
        bias = (wr_c - mp) * 100
        print(f"  {metric:8s}  N={len(cp):>4}  mean_price={mp:.3f}  win_rate={wr_c:.3f}  Brier={bs_c:.4f}  bias={bias:+.1f}pp")

    # By direction
    print(f"\n  By Direction (YES side):")
    dir_groups: Dict[str, list] = defaultdict(list)
    for t in trades:
        dir_groups[t["direction"]].append((t["entry_price"], t["outcome"]))
    for d, cp in sorted(dir_groups.items()):
        mp = sum(p for p, _ in cp) / len(cp)
        wr_c = win_rate(cp)
        bs_c = brier_score(cp)
        bias = (wr_c - mp) * 100
        print(f"  YES={d:5s}  N={len(cp):>4}  mean_price={mp:.3f}  win_rate={wr_c:.3f}  Brier={bs_c:.4f}  bias={bias:+.1f}pp")

    # Price bucket profitability (if you had blindly entered every market)
    # Profit = outcome - entry_price (net, ignoring fees)
    print(f"\n  Price Bucket Profitability (blind entry at ask, before fees):")
    print(f"  {'Bucket':10s} {'N':>5} {'Mean PnL':>10} {'Win%':>7} {'Breakeven%':>12}")
    for row in curve:
        if row["n"] == 0:
            continue
        pts = [(t["entry_price"], t["outcome"]) for t in trades
               if row["bucket_lo"] <= t["entry_price"] < row["bucket_hi"]]
        if not pts:
            continue
        mean_pnl = sum(o - p for p, o in pts) / len(pts)
        wr_b = sum(o for _, o in pts) / len(pts)
        be   = sum(market_breakeven_win_rate(p) for p, _ in pts) / len(pts)
        print(f"  {row['bucket_lo']:.0%}–{row['bucket_hi']:.0%}    {len(pts):>5} {mean_pnl:>+10.4f} {wr_b:>7.3f} {be:>12.3f}")

    # Systematic mispricing assessment
    print(f"\n  Systematic Mispricing Assessment:")
    total_pnl = sum(t["outcome"] - t["entry_price"] for t in trades)
    total_pnl_net = sum(
        (t["outcome"] * (1 - 0.07) - t["entry_price"]) if t["outcome"] == 1
        else -t["entry_price"]
        for t in trades
    )
    print(f"  Total P&L (gross, $1 per contract): ${total_pnl:+.2f}")
    print(f"  Total P&L (net of 7% Kalshi fee):   ${total_pnl_net:+.2f}")
    print(f"  P&L per trade (net):                ${total_pnl_net/len(trades):+.4f}")
    brier_skill = (bs_random - bs) / bs_random * 100
    print(f"  Brier skill score (vs 50/50 random):{brier_skill:+.1f}%")
    if abs(total_pnl_net / len(trades)) < 0.01:
        print(f"\n  VERDICT: Market prices appear WELL CALIBRATED (P&L near 0).")
        print(f"  Blind-entry strategy is roughly breakeven. Any edge must come from")
        print(f"  having better probability estimates than the market price, not from")
        print(f"  a systematic bias in how Kalshi prices these contracts.")
    elif total_pnl_net > 0:
        print(f"\n  VERDICT: Market shows POSITIVE bias for YES buyers (underpriced YES).")
        print(f"  Systematically buying YES would have been profitable even without a model.")
    else:
        print(f"\n  VERDICT: Market shows NEGATIVE bias for YES buyers (overpriced YES).")
        print(f"  Systematically buying NO would have been profitable without a model.")

    # ------------------------------------------------------------------
    # Save JSON summary
    # ------------------------------------------------------------------
    summary = {
        "date_run":        date.today().isoformat(),
        "n_markets":       len(trades),
        "date_range":      [min(t["open_time"] for t in trades), max(t["open_time"] for t in trades)],
        "mean_entry_price": round(mean_price, 4),
        "actual_win_rate":  round(wr, 4),
        "brier_score":      round(bs, 4),
        "brier_ci_95":      [round(bs_ci_lo, 4), round(bs_ci_hi, 4)],
        "brier_implied":    round(bs_implied, 4),
        "brier_skill_pct":  round(brier_skill, 2),
        "total_pnl_gross":  round(total_pnl, 2),
        "total_pnl_net":    round(total_pnl_net, 2),
        "pnl_per_trade":    round(total_pnl_net / len(trades), 4),
        "bias_pp":          round((wr - mean_price) * 100, 2),
        "calibration_curve": curve,
        "by_city":   {
            city: {
                "n": len(cp),
                "mean_price": round(sum(p for p, _ in cp)/len(cp), 4),
                "win_rate":   round(win_rate(cp), 4),
                "brier":      round(brier_score(cp), 4),
                "bias_pp":    round((win_rate(cp) - sum(p for p, _ in cp)/len(cp))*100, 2),
            }
            for city, cp in city_groups.items()
        },
        "by_direction": {
            d: {
                "n": len(cp),
                "mean_price": round(sum(p for p, _ in cp)/len(cp), 4),
                "win_rate":   round(win_rate(cp), 4),
                "brier":      round(brier_score(cp), 4),
            }
            for d, cp in dir_groups.items()
        },
        "price_sources": dict(price_sources),
    }

    json_path = OUT_DIR / "market_efficiency_summary.json"
    json_path.write_text(json.dumps(summary, indent=2))
    print(f"\n  Summary saved to: {json_path}")
    print(f"  Trades CSV saved to: {csv_path}")
    print(f"{'='*70}\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kalshi weather market efficiency audit")
    parser.add_argument(
        "--series",
        default=",".join(DEFAULT_SERIES),
        help="Comma-separated series tickers (default: all bot-traded series)",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=15,
        help="Max pages per series (200 markets/page, default 15 = up to 3000 per series)",
    )
    args = parser.parse_args()

    series_list = [s.strip() for s in args.series.split(",") if s.strip()]
    asyncio.run(main(series_list, args.max_pages))
