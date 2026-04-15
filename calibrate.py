"""
Historical calibration backtest for the Kalshi weather arb bot.

Fetches the last 90 days of SETTLED Kalshi weather markets, runs the bot's
generate_weather_signal logic on each (using the market's target_date and
threshold), and compares model probability vs actual outcome.

Outputs:
  - Per-city Brier score
  - Win rate at different edge thresholds (5%, 8%, 12%, 15%)
  - Calibration curve (model prob buckets vs actual outcome frequency)
  - Saves calibration_results.json at the project root

Usage:
    python calibrate.py [--days N] [--out PATH]
"""
import argparse
import asyncio
import json
import logging
import math
import sys
from collections import defaultdict
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Tuple

from dotenv import load_dotenv
load_dotenv()

from backend.data.kalshi_client import KalshiClient, kalshi_credentials_present
from backend.data.kalshi_markets import (
    KNOWN_SERIES_MAP,
    MONTH_ABBR,
    NON_WEATHER_BLACKLIST,
    WEATHER_PREFIXES,
    _parse_temp_ticker,
    _parse_rain_ticker,
)
from backend.core.weather_signals import generate_weather_signal
from backend.data.weather_markets import WeatherMarket

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    stream=sys.stderr,
)
logger = logging.getLogger("calibrate")


# ── Kalshi settled market fetcher ─────────────────────────────────────────────

async def fetch_settled_markets(
    client: KalshiClient,
    series_ticker: str,
    city_key: str,
    metric: str,
    since: date,
) -> List[dict]:
    """Fetch settled markets for a given series since `since` date."""
    markets = []
    cursor = None

    while True:
        params: dict = {
            "series_ticker": series_ticker,
            "status": "finalized",
            "limit": 200,
        }
        if cursor:
            params["cursor"] = cursor

        try:
            data = await client.get_markets(params)
        except Exception as e:
            logger.warning(f"Failed to fetch settled markets for {series_ticker}: {e}")
            break

        raw = data.get("markets", [])
        for m in raw:
            # Filter to the look-back window
            close_time = m.get("close_time") or m.get("expiration_time", "")
            if close_time:
                try:
                    close_date = datetime.fromisoformat(
                        close_time.replace("Z", "+00:00")
                    ).date()
                    if close_date < since:
                        continue
                except Exception:
                    pass

            ticker = m.get("ticker", "")
            title  = m.get("title", ticker)

            # Parse into structured form
            if metric == "rain":
                parsed = _parse_rain_ticker(ticker, city_key, title)
            else:
                if "-B" in ticker:
                    continue
                parsed = _parse_temp_ticker(ticker, city_key, metric, title)

            if not parsed:
                continue

            # Determine actual outcome (YES or NO resolved)
            result = m.get("result", "")          # "yes" or "no"
            yes_price_close = None

            # Kalshi may give yes_price at settlement = 1.0 (yes won) or 0.0 (no won)
            sp = m.get("last_price_dollars") or m.get("last_price")
            if sp is not None:
                try:
                    yes_price_close = float(sp) if isinstance(sp, (int, float)) else float(sp) / 100.0
                    if yes_price_close > 1.0:
                        yes_price_close /= 100.0
                except Exception:
                    pass

            if result == "yes":
                actual_outcome = 1
            elif result == "no":
                actual_outcome = 0
            elif yes_price_close is not None:
                actual_outcome = 1 if yes_price_close >= 0.95 else (0 if yes_price_close <= 0.05 else None)
            else:
                actual_outcome = None

            if actual_outcome is None:
                continue  # ambiguous — skip

            markets.append({
                "ticker": ticker,
                "title": title,
                "city_key": city_key,
                "metric": metric,
                "target_date": parsed["target_date"],
                "threshold_f": parsed["threshold_f"],
                "direction": parsed["direction"],
                "actual_outcome": actual_outcome,
                "market_price_at_entry": None,  # we use what the API gives at query time
            })

        cursor = data.get("cursor")
        if not cursor or not raw:
            break

    return markets


# ── Model prediction runner ───────────────────────────────────────────────────

async def run_model_for_settled(entry: dict) -> Optional[Tuple[float, int]]:
    """
    Run generate_weather_signal for a settled market.
    Returns (model_yes_prob, actual_outcome) or None if signal can't be generated.

    Note: we build a synthetic WeatherMarket with yes_price=0.50 (neutral)
    so the edge calculation doesn't affect model_probability itself.
    """
    # Build a synthetic market object
    market = WeatherMarket(
        slug=entry["ticker"],
        market_id=entry["ticker"],
        platform="kalshi",
        title=entry["title"],
        city_key=entry["city_key"],
        city_name=entry["city_key"],
        target_date=entry["target_date"],
        threshold_f=entry["threshold_f"],
        metric=entry["metric"],
        direction=entry["direction"],
        yes_price=0.50,   # neutral — we only care about model_probability
        no_price=0.50,
        volume=0.0,
    )

    try:
        signal = await generate_weather_signal(market)
    except Exception as e:
        logger.debug(f"Signal generation failed for {entry['ticker']}: {e}")
        return None

    if signal is None:
        return None

    return (signal.model_probability, entry["actual_outcome"])


# ── Calibration metrics ───────────────────────────────────────────────────────

def brier_score(predictions: List[Tuple[float, int]]) -> float:
    """Mean squared error: (1/N) * sum((prob - outcome)^2)."""
    if not predictions:
        return float("nan")
    return sum((p - o) ** 2 for p, o in predictions) / len(predictions)


def calibration_curve(
    predictions: List[Tuple[float, int]],
    n_buckets: int = 10,
) -> List[dict]:
    """
    Bin predictions into probability buckets and compute actual frequency.
    Returns list of {bucket_mid, model_mean, actual_freq, count}.
    """
    buckets: Dict[int, List] = defaultdict(list)
    for prob, outcome in predictions:
        bucket = min(int(prob * n_buckets), n_buckets - 1)
        buckets[bucket].append((prob, outcome))

    result = []
    for i in range(n_buckets):
        items = buckets.get(i, [])
        if not items:
            continue
        mid = (i + 0.5) / n_buckets
        model_mean = sum(p for p, _ in items) / len(items)
        actual_freq = sum(o for _, o in items) / len(items)
        result.append({
            "bucket_mid": round(mid, 2),
            "model_mean": round(model_mean, 3),
            "actual_freq": round(actual_freq, 3),
            "count": len(items),
        })
    return result


def win_rate_at_threshold(
    predictions: List[Tuple[float, int]],
    threshold: float,
) -> Optional[dict]:
    """
    For signals where |model_prob - 0.5| >= threshold (edge >= threshold):
    compute win rate (how often the higher-probability side won).
    """
    subset = []
    for prob, outcome in predictions:
        edge = abs(prob - 0.5)
        if edge < threshold:
            continue
        # "win" = model's favoured side was correct
        model_yes = prob >= 0.5
        win = (model_yes and outcome == 1) or (not model_yes and outcome == 0)
        subset.append(int(win))

    if not subset:
        return None
    return {
        "threshold": threshold,
        "count": len(subset),
        "win_rate": round(sum(subset) / len(subset), 3),
    }


# ── Main ──────────────────────────────────────────────────────────────────────

async def main(lookback_days: int, out_path: str):
    if not kalshi_credentials_present():
        print("ERROR: Kalshi credentials not configured. Set KALSHI_API_KEY_ID and KALSHI_PRIVATE_KEY_PATH.", file=sys.stderr)
        sys.exit(1)

    client = KalshiClient()
    since = date.today() - timedelta(days=lookback_days)

    logger.info(f"Calibration backtest: fetching settled markets from {since} onwards ({lookback_days} days)")

    # Discover active series (we scan all, not just currently open ones)
    try:
        data = await client.get("/series", {"limit": 500})
        all_series = data.get("series", [])
    except Exception as e:
        logger.error(f"Series discovery failed: {e}")
        sys.exit(1)

    # Map series ticker -> (city_key, metric)
    target_series = []
    seen = set()
    for s in all_series:
        ticker = s.get("ticker", "")
        if not any(ticker.startswith(p) for p in WEATHER_PREFIXES):
            continue
        if ticker in NON_WEATHER_BLACKLIST:
            continue
        mapping = KNOWN_SERIES_MAP.get(ticker)
        if mapping is None:
            continue
        city_key, metric = mapping
        dedup_key = (city_key, metric)
        if dedup_key in seen:
            continue
        seen.add(dedup_key)
        target_series.append((ticker, city_key, metric))

    logger.info(f"Found {len(target_series)} series to scan for settled markets")

    # Fetch all settled markets
    all_entries = []
    for series_ticker, city_key, metric in target_series:
        entries = await fetch_settled_markets(client, series_ticker, city_key, metric, since)
        logger.info(f"  {series_ticker} ({city_key}/{metric}): {len(entries)} settled markets")
        all_entries.extend(entries)

    logger.info(f"Total settled markets found: {len(all_entries)}")

    if not all_entries:
        logger.warning("No settled markets found — cannot calibrate. The look-back window may be too short or the API may not return historical data.")
        results = {"error": "no settled markets found", "lookback_days": lookback_days}
        with open(out_path, "w") as f:
            json.dump(results, f, indent=2)
        print(f"Results saved to {out_path}")
        return

    # Run model for each settled market
    logger.info("Running model predictions for each settled market...")
    predictions_by_city: Dict[str, List[Tuple[float, int]]] = defaultdict(list)
    all_predictions: List[Tuple[float, int]] = []
    skipped = 0

    for entry in all_entries:
        result = await run_model_for_settled(entry)
        if result is None:
            skipped += 1
            continue
        model_prob, actual = result
        predictions_by_city[entry["city_key"]].append((model_prob, actual))
        all_predictions.append((model_prob, actual))

    logger.info(f"Model predictions: {len(all_predictions)} completed, {skipped} skipped")

    if not all_predictions:
        logger.warning("No predictions could be generated — model may lack data for historical dates.")
        results = {"error": "no predictions generated", "settled_markets": len(all_entries)}
        with open(out_path, "w") as f:
            json.dump(results, f, indent=2)
        print(f"Results saved to {out_path}")
        return

    # ── Compute metrics ───────────────────────────────────────────────────────
    overall_brier = brier_score(all_predictions)
    logger.info(f"Overall Brier score: {overall_brier:.4f} (n={len(all_predictions)})")

    per_city_brier = {}
    for city, preds in sorted(predictions_by_city.items()):
        bs = brier_score(preds)
        per_city_brier[city] = {"brier_score": round(bs, 4), "n": len(preds)}
        logger.info(f"  {city}: Brier={bs:.4f}  (n={len(preds)})")

    calibration = calibration_curve(all_predictions)

    edge_thresholds = [0.05, 0.08, 0.12, 0.15, 0.20]
    win_rates = []
    for t in edge_thresholds:
        wr = win_rate_at_threshold(all_predictions, t)
        if wr:
            win_rates.append(wr)
            logger.info(f"  Edge>={t:.0%}: win_rate={wr['win_rate']:.1%}  (n={wr['count']})")

    results = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "lookback_days": lookback_days,
        "since": since.isoformat(),
        "total_predictions": len(all_predictions),
        "skipped": skipped,
        "overall_brier_score": round(overall_brier, 4),
        "per_city_brier": per_city_brier,
        "win_rates_by_edge_threshold": win_rates,
        "calibration_curve": calibration,
    }

    with open(out_path, "w") as f:
        json.dump(results, f, indent=2, default=str)

    print(f"\n=== CALIBRATION RESULTS ===")
    print(f"Settled markets: {len(all_entries)}  |  Predictions: {len(all_predictions)}")
    print(f"Overall Brier score: {overall_brier:.4f}  (0=perfect, 0.25=random)")
    print(f"\nPer-city Brier scores:")
    for city, v in per_city_brier.items():
        print(f"  {city:20s}: {v['brier_score']:.4f}  (n={v['n']})")
    print(f"\nWin rates by edge threshold:")
    for wr in win_rates:
        print(f"  edge >= {wr['threshold']:.0%}: {wr['win_rate']:.1%}  (n={wr['count']})")
    print(f"\nResults saved to {out_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Historical calibration backtest for weather arb bot")
    parser.add_argument("--days", type=int, default=90, help="Look-back window in days (default: 90)")
    parser.add_argument("--out", type=str, default="calibration_results.json", help="Output JSON path")
    args = parser.parse_args()

    asyncio.run(main(args.days, args.out))
