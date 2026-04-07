"""Kalshi weather market fetcher — scans all configured weather series."""
import logging
import re
from datetime import date
from typing import Dict, List, Optional

from backend.data.kalshi_client import KalshiClient, kalshi_credentials_present
from backend.data.weather_markets import WeatherMarket

logger = logging.getLogger("weatherbot")

# ── Series configuration ────────────────────────────────────────────────────
# Each entry: (series_ticker, city_key, metric)
# metric: "high" or "low"
# To add a new series, just append a tuple here.
WEATHER_SERIES: List[tuple] = [
    # Daily HIGH temperature
    ("KXHIGHNY",  "nyc",         "high"),
    ("KXHIGHCHI", "chicago",     "high"),
    ("KXHIGHMIA", "miami",       "high"),
    ("KXHIGHLAX", "los_angeles", "high"),
    ("KXHIGHDEN", "denver",      "high"),
    # Daily LOW temperature
    ("KXLOWNY",   "nyc",         "low"),
    ("KXLOWCHI",  "chicago",     "low"),
    ("KXLOWMIA",  "miami",       "low"),
    ("KXLOWLAX",  "los_angeles", "low"),
    ("KXLOWDEN",  "denver",      "low"),
]

CITY_NAMES: Dict[str, str] = {
    "nyc":         "New York",
    "chicago":     "Chicago",
    "miami":       "Miami",
    "los_angeles": "Los Angeles",
    "denver":      "Denver",
}

# Month abbreviation mapping for ticker parsing
MONTH_ABBR = {
    "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
    "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12,
}


def _parse_kalshi_ticker(ticker: str, city_key: str, metric: str) -> Optional[dict]:
    """
    Parse a Kalshi bracket ticker into market parameters.

    Format: KXHIGHNY-26MAR01-B45.5
      - 26MAR01 = 2026-03-01
      - B45.5 = bottom boundary at 45.5°F → direction "above"
      - T45.5 = top boundary → direction "below"
    """
    match = re.match(
        r'^[A-Z]+-(\d{2})([A-Z]{3})(\d{2})-([BT])([\d.]+)$',
        ticker,
    )
    if not match:
        return None

    yy, mon_str, dd = int(match.group(1)), match.group(2), int(match.group(3))
    boundary_type = match.group(4)
    threshold = float(match.group(5))

    month = MONTH_ABBR.get(mon_str)
    if not month:
        return None

    try:
        target_date = date(2000 + yy, month, dd)
    except ValueError:
        return None

    direction = "above" if boundary_type == "B" else "below"

    return {
        "target_date": target_date,
        "threshold_f": threshold,
        "metric": metric,
        "direction": direction,
    }


async def fetch_kalshi_weather_markets(
    city_keys: Optional[List[str]] = None,
) -> List[WeatherMarket]:
    """
    Fetch open weather temperature markets from Kalshi for all configured series.

    Pass city_keys to filter to specific cities (or None for all).
    """
    if not kalshi_credentials_present():
        return []

    client = KalshiClient()
    markets: List[WeatherMarket] = []
    today = date.today()

    for series_ticker, city_key, metric in WEATHER_SERIES:
        if city_keys and city_key not in city_keys:
            continue

        city_name = CITY_NAMES.get(city_key, city_key)
        cursor = None

        try:
            while True:
                params: dict = {
                    "series_ticker": series_ticker,
                    "status": "open",
                    "limit": 200,
                }
                if cursor:
                    params["cursor"] = cursor

                data = await client.get_markets(params)
                raw_markets = data.get("markets", [])

                for m in raw_markets:
                    ticker = m.get("ticker", "")
                    parsed = _parse_kalshi_ticker(ticker, city_key, metric)
                    if not parsed:
                        continue
                    if parsed["target_date"] < today:
                        continue

                    yes_price = (m.get("yes_ask") or 0) / 100.0
                    no_price = (m.get("no_ask") or 0) / 100.0

                    if yes_price <= 0:
                        yes_price = (m.get("last_price") or 50) / 100.0
                    if no_price <= 0:
                        no_price = 1.0 - yes_price

                    # Skip fully resolved or near-certain
                    if yes_price > 0.98 or yes_price < 0.02:
                        continue

                    markets.append(WeatherMarket(
                        slug=ticker,
                        market_id=ticker,
                        platform="kalshi",
                        title=m.get("title", ticker),
                        city_key=city_key,
                        city_name=city_name,
                        target_date=parsed["target_date"],
                        threshold_f=parsed["threshold_f"],
                        metric=parsed["metric"],
                        direction=parsed["direction"],
                        yes_price=yes_price,
                        no_price=no_price,
                        volume=float(m.get("volume", 0) or 0),
                    ))

                cursor = data.get("cursor")
                if not cursor or not raw_markets:
                    break

        except Exception as e:
            logger.warning(f"Failed to fetch Kalshi markets for {series_ticker}: {e}")

    logger.info(f"Found {len(markets)} Kalshi weather markets across all series")
    return markets
