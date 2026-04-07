"""Kalshi weather market fetcher — scans all configured weather series."""
import logging
import re
from datetime import date
from typing import Dict, List, Optional

from backend.data.kalshi_client import KalshiClient, kalshi_credentials_present
from backend.data.weather_markets import WeatherMarket

logger = logging.getLogger("weatherbot")

# ── Series configuration ─────────────────────────────────────────────────────
# Each entry: (series_ticker, city_key, metric)
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

MONTH_ABBR = {
    "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
    "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12,
}


def _parse_market_title_direction(title: str) -> Optional[str]:
    """
    Extract direction from the Kalshi market title.
    Titles explicitly say 'be >X' or 'be <X' or 'be X-Y'.

    Returns "above", "below", or None (for bracket range markets).
    """
    t = title.lower()
    # Bracket range: "be 48-49°" or "be between 48 and 49"
    if re.search(r'be \d+[-–]\d+', t):
        return None   # range bracket — skip
    if re.search(r'between \d+ and \d+', t):
        return None

    # Tail markets: "be >X" / ">= X" = above, "be <X" / "<= X" = below
    if re.search(r'be\s*[>≥]|above|exceed|over', t):
        return "above"
    if re.search(r'be\s*[<≤]|below|under|less than', t):
        return "below"
    return None


def _parse_kalshi_ticker(ticker: str, city_key: str, metric: str, title: str = "") -> Optional[dict]:
    """
    Parse a Kalshi weather ticker.

    Kalshi KXHIGH series has two market types:
      T tickers = tail markets: "will high be >X°" or "will high be <X°"  (binary, tradeable)
      B tickers = bracket markets: "will high be X-(X+1)°"                (range, skip)

    Direction is read from the market title, which is authoritative.
    """
    match = re.match(
        r'^[A-Z]+-(\d{2})([A-Z]{3})(\d{2})-([BT])([\d.]+)$',
        ticker,
    )
    if not match:
        return None

    yy, mon_str, dd = int(match.group(1)), match.group(2), int(match.group(3))
    boundary_type = match.group(4)  # "B" or "T"
    threshold = float(match.group(5))

    month = MONTH_ABBR.get(mon_str)
    if not month:
        return None

    try:
        target_date = date(2000 + yy, month, dd)
    except ValueError:
        return None

    # B markets are 1°F bracket ranges — not binary, skip them
    if boundary_type == "B":
        return None

    # T markets are tail markets — determine above/below from the title
    direction = _parse_market_title_direction(title)
    if direction is None:
        # Title didn't parse cleanly — fall back to heuristic:
        # Lower-valued T = lower tail = below, higher-valued T = upper tail = above
        # (not reliable, so skip rather than guess)
        logger.debug(f"Skipping {ticker}: can't determine direction from title '{title}'")
        return None

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
    Fetch open binary weather temperature markets from Kalshi.

    Only returns T (tail) markets — the binary above/below ones.
    Skips B (bracket) range markets which require a different probability model.
    Skips markets with no price data (not yet actively quoted).
    """
    if not kalshi_credentials_present():
        return []

    client = KalshiClient()
    markets: List[WeatherMarket] = []
    today = date.today()
    skipped_no_price = 0
    skipped_bracket = 0

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
                    title = m.get("title", ticker)

                    # Skip bracket (B) markets at parse time
                    if "-B" in ticker:
                        skipped_bracket += 1
                        continue

                    parsed = _parse_kalshi_ticker(ticker, city_key, metric, title)
                    if not parsed:
                        skipped_bracket += 1
                        continue

                    if parsed["target_date"] < today:
                        continue

                    # Price resolution: Kalshi returns *_dollars fields (e.g. "0.0900" = 9¢)
                    # Prefer ask; fall back to last traded price; skip if no data at all.
                    yes_ask_d   = m.get("yes_ask_dollars")
                    no_ask_d    = m.get("no_ask_dollars")
                    last_d      = m.get("last_price_dollars")
                    # Also check legacy integer cent fields as fallback
                    yes_ask_c   = m.get("yes_ask")
                    no_ask_c    = m.get("no_ask")
                    last_c      = m.get("last_price")

                    if yes_ask_d is not None:
                        yes_price = float(yes_ask_d)
                    elif yes_ask_c is not None:
                        yes_price = yes_ask_c / 100.0
                    elif last_d is not None:
                        yes_price = float(last_d)
                    elif last_c is not None:
                        yes_price = last_c / 100.0
                    else:
                        skipped_no_price += 1
                        continue   # no price data — can't trade

                    if no_ask_d is not None:
                        no_price = float(no_ask_d)
                    elif no_ask_c is not None:
                        no_price = no_ask_c / 100.0
                    else:
                        no_price = 1.0 - yes_price

                    # Skip resolved or near-certain markets
                    if yes_price > 0.97 or yes_price < 0.03:
                        continue

                    markets.append(WeatherMarket(
                        slug=ticker,
                        market_id=ticker,
                        platform="kalshi",
                        title=title,
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

    logger.info(
        f"Found {len(markets)} tradeable Kalshi weather markets "
        f"(skipped {skipped_bracket} brackets, {skipped_no_price} with no price)"
    )
    return markets
