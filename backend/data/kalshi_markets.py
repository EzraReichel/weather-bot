"""
Kalshi weather market fetcher — dynamically discovers all active weather series.

Replaces the hardcoded 5-city list with a live API scan of every series ticker
that starts with KXHIGH, KXLOW, or KXRAIN. Unknown cities fall back to
lat/lon geocoding from the series title if not in KNOWN_SERIES_MAP.
"""
import logging
import re
from datetime import date
from typing import Dict, List, Optional, Tuple

from dataclasses import dataclass, field

from backend.data.kalshi_client import KalshiClient, kalshi_credentials_present
from backend.data.weather_markets import WeatherMarket


@dataclass
class FilteredMarket:
    ticker: str
    city: str
    title: str
    reason: str           # "bracket", "no_price", "low_ask", "low_volume", "expired"
    ask_size: float = 0.0
    volume_24h: float = 0.0
    yes_price: float = 0.0


@dataclass
class MarketFetchReport:
    markets: list = field(default_factory=list)          # List[WeatherMarket] — passed all filters
    filtered: list = field(default_factory=list)         # List[FilteredMarket]
    series_scanned: int = 0
    total_raw: int = 0

logger = logging.getLogger("weatherbot")

MIN_ASK_SIZE   = 50     # minimum contracts on the yes ask
MIN_VOLUME_24H = 200    # minimum 24h volume ($1 face value per contract)
                        # Lower than original 1000 to include next-day markets that haven't
                        # fully traded yet but have real ask depth

MONTH_ABBR = {
    "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
    "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12,
}

# ── Known series → city_key + metric mapping ──────────────────────────────────
# (series_ticker, city_key, metric)
# city_key must exist in backend/data/weather.py CITY_CONFIG.
# If a new series appears that isn't here, it's skipped with a debug log.
KNOWN_SERIES_MAP: Dict[str, Tuple[str, str]] = {
    # HIGH temperature
    "KXHIGHNY":       ("nyc",           "high"),
    "KXHIGHNY0":      ("nyc",           "high"),
    "KXHIGHCHI":      ("chicago",       "high"),
    "KXHIGHMIA":      ("miami",         "high"),
    "KXHIGHLAX":      ("los_angeles",   "high"),
    "KXHIGHDEN":      ("denver",        "high"),
    "KXHIGHAUS":      ("austin",        "high"),
    "KXHIGHHOU":      ("houston",       "high"),
    "KXHIGHOU":       ("houston",       "high"),   # legacy ticker
    "KXHIGHTBOS":     ("boston",        "high"),
    "KXHIGHTDC":      ("washington_dc", "high"),
    "KXHIGHTPHX":     ("phoenix",       "high"),
    "KXHIGHTSEA":     ("seattle",       "high"),
    "KXHIGHTSFO":     ("san_francisco", "high"),
    "KXHIGHTATL":     ("atlanta",       "high"),
    "KXHIGHTDAL":     ("dallas",        "high"),
    "KXHIGHTLV":      ("las_vegas",     "high"),
    "KXHIGHTMIN":     ("minneapolis",   "high"),
    "KXHIGHTNOLA":    ("new_orleans",   "high"),
    "KXHIGHTOKC":     ("oklahoma_city", "high"),
    "KXHIGHTSATX":    ("san_antonio",   "high"),
    "KXHIGHTHOU":     ("houston",       "high"),
    "KXHIGHPHIL":     ("philadelphia",  "high"),
    "KXHIGHTEMPDEN":  ("denver",        "high"),   # legacy
    # LOW temperature
    "KXLOWNY":        ("nyc",           "low"),
    "KXLOWNYC":       ("nyc",           "low"),
    "KXLOWTNYC":      ("nyc",           "low"),
    "KXLOWCHI":       ("chicago",       "low"),
    "KXLOWTCHI":      ("chicago",       "low"),
    "KXLOWMIA":       ("miami",         "low"),
    "KXLOWTMIA":      ("miami",         "low"),
    "KXLOWLAX":       ("los_angeles",   "low"),
    "KXLOWTLAX":      ("los_angeles",   "low"),
    "KXLOWDEN":       ("denver",        "low"),
    "KXLOWTDEN":      ("denver",        "low"),
    "KXLOWAUS":       ("austin",        "low"),
    "KXLOWTAUS":      ("austin",        "low"),
    "KXLOWPHIL":      ("philadelphia",  "low"),
    "KXLOWTPHIL":     ("philadelphia",  "low"),
    "KXLOWTBOS":      ("boston",        "low"),
    "KXLOWTDC":       ("washington_dc", "low"),
    "KXLOWTPHX":      ("phoenix",       "low"),
    "KXLOWTSEA":      ("seattle",       "low"),
    "KXLOWTSFO":      ("san_francisco", "low"),
    "KXLOWTATL":      ("atlanta",       "low"),
    "KXLOWTDAL":      ("dallas",        "low"),
    "KXLOWTLV":       ("las_vegas",     "low"),
    "KXLOWTMIN":      ("minneapolis",   "low"),
    "KXLOWTNOLA":     ("new_orleans",   "low"),
    "KXLOWTOKC":      ("oklahoma_city", "low"),
    "KXLOWTSATX":     ("san_antonio",   "low"),
    "KXLOWTHOU":      ("houston",       "low"),
    # RAIN — binary yes/no (will it rain today)
    "KXRAINNY":       ("nyc",           "rain"),
    "KXRAINNYC":      ("nyc",           "rain"),
    "KXRAINCHIM":     ("chicago",       "rain"),
    "KXRAINMIA":      ("miami",         "rain"),
    "KXRAINMIAM":     ("miami",         "rain"),
    "KXRAINLAXM":     ("los_angeles",   "rain"),
    "KXRAINDENM":     ("denver",        "rain"),
    "KXRAINAUSM":     ("austin",        "rain"),
    "KXRAINHOUM":     ("houston",       "rain"),
    "KXRAINHOU":      ("houston",       "rain"),
    "KXRAINSEAM":     ("seattle",       "rain"),
    "KXRAINSEA":      ("seattle",       "rain"),
    "KXRAINSFOM":     ("san_francisco", "rain"),
    "KXRAINDALM":     ("dallas",        "rain"),
    "KXRAINNO":       ("new_orleans",   "rain"),
    # Monthly / multi-day accumulation rain series — different resolution model, skip for now
    # "KXRAINNYCM": monthly, not daily binary
}

# Prefixes we scan — non-weather series with these prefixes are filtered
# by KNOWN_SERIES_MAP lookup, so false matches are safely skipped.
WEATHER_PREFIXES = ("KXHIGH", "KXLOW", "KXRAIN")

# Series that are definitively NOT temperature/rain markets (avoid scanning)
NON_WEATHER_BLACKLIST = {
    "KXHIGHINFLATION", "KXHIGHMOVDJT", "KXHIGHMOVKH",
    "KXHIGHUS", "KXHIGHNYD",  # directional/national aggregates
    "KXLOWESTRATE",            # Fed funds rate
    "KXRAINNOSB",              # Super Bowl one-off
}

# ── Series discovery ──────────────────────────────────────────────────────────

async def discover_active_series(client: KalshiClient) -> List[Tuple[str, str, str]]:
    """
    Fetch all Kalshi series, filter to active weather series, return as
    list of (series_ticker, city_key, metric) tuples.

    Only returns series that have a mapping in KNOWN_SERIES_MAP.
    Unknown series are logged at DEBUG so we can add them later.
    """
    try:
        data = await client.get("/series", {"limit": 500})
        all_series = data.get("series", [])
    except Exception as e:
        logger.warning(f"Series discovery failed: {e} — falling back to hardcoded list")
        return _hardcoded_fallback()

    active: List[Tuple[str, str, str]] = []
    seen_city_metric: set = set()   # deduplicate (city_key, metric) pairs

    for s in all_series:
        ticker = s.get("ticker", "")
        if not any(ticker.startswith(p) for p in WEATHER_PREFIXES):
            continue
        if ticker in NON_WEATHER_BLACKLIST:
            continue

        mapping = KNOWN_SERIES_MAP.get(ticker)
        if mapping is None:
            logger.debug(f"Unknown series {ticker!r} ({s.get('title','')}) — add to KNOWN_SERIES_MAP to enable")
            continue

        city_key, metric = mapping
        dedup_key = (city_key, metric)
        if dedup_key in seen_city_metric:
            # Skip duplicate series for the same city+metric (e.g. KXHIGHNY and KXHIGHNY0)
            logger.debug(f"Skipping duplicate series {ticker} for {city_key}/{metric}")
            continue

        seen_city_metric.add(dedup_key)
        active.append((ticker, city_key, metric))

    logger.info(
        f"Series discovery: {len(all_series)} total → {len(active)} active weather series "
        f"({sum(1 for _,_,m in active if m=='high')} high, "
        f"{sum(1 for _,_,m in active if m=='low')} low, "
        f"{sum(1 for _,_,m in active if m=='rain')} rain)"
    )
    return active


def _hardcoded_fallback() -> List[Tuple[str, str, str]]:
    """Emergency fallback if the /series endpoint fails."""
    return [
        ("KXHIGHNY",  "nyc",         "high"),
        ("KXHIGHCHI", "chicago",     "high"),
        ("KXHIGHMIA", "miami",       "high"),
        ("KXHIGHLAX", "los_angeles", "high"),
        ("KXHIGHDEN", "denver",      "high"),
        ("KXLOWNY",   "nyc",         "low"),
        ("KXLOWCHI",  "chicago",     "low"),
        ("KXLOWMIA",  "miami",       "low"),
        ("KXLOWLAX",  "los_angeles", "low"),
        ("KXLOWDEN",  "denver",      "low"),
    ]


# ── Ticker parsers ────────────────────────────────────────────────────────────

def _parse_market_title_direction(title: str) -> Optional[str]:
    t = title.lower()
    if re.search(r'be \d+[-–]\d+', t) or re.search(r'between \d+ and \d+', t):
        return None
    if re.search(r'be\s*[>≥]|above|exceed|over', t):
        return "above"
    if re.search(r'be\s*[<≤]|below|under|less than', t):
        return "below"
    return None


def _parse_temp_ticker(ticker: str, city_key: str, metric: str, title: str = "") -> Optional[dict]:
    """Parse a KXHIGH/KXLOW temperature ticker."""
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

    if boundary_type == "B":
        return None   # bracket range — not binary

    direction = _parse_market_title_direction(title)
    if direction is None:
        logger.debug(f"Skipping {ticker}: can't determine direction from title '{title}'")
        return None

    return {
        "target_date": target_date,
        "threshold_f": threshold,
        "metric": metric,
        "direction": direction,
        "market_type": "temperature",
    }


def _parse_rain_ticker(ticker: str, city_key: str, title: str = "") -> Optional[dict]:
    """
    Parse a KXRAIN ticker.
    Format: SERIES-YYMMMDD-T0  (threshold is always 0 for binary rain markets)
    """
    match = re.match(
        r'^[A-Z]+-(\d{2})([A-Z]{3})(\d{2})-T0$',
        ticker,
    )
    if not match:
        return None

    yy, mon_str, dd = int(match.group(1)), match.group(2), int(match.group(3))
    month = MONTH_ABBR.get(mon_str)
    if not month:
        return None

    try:
        target_date = date(2000 + yy, month, dd)
    except ValueError:
        return None

    return {
        "target_date": target_date,
        "threshold_f": 0.0,
        "metric": "rain",
        "direction": "above",   # YES = will rain
        "market_type": "rain",
    }


# ── Main fetch function ───────────────────────────────────────────────────────

async def fetch_kalshi_weather_markets(
    city_keys: Optional[List[str]] = None,
) -> "MarketFetchReport":
    """
    Dynamically discover all active Kalshi weather series and fetch their
    open binary (T-ticker) markets. Returns a MarketFetchReport with both
    the passing markets and the full filtered list for the daily report.
    """
    report = MarketFetchReport()

    if not kalshi_credentials_present():
        return report

    client = KalshiClient()
    today = date.today()

    from backend.data.weather import CITY_CONFIG

    active_series = await discover_active_series(client)
    report.series_scanned = len(active_series)

    for series_ticker, city_key, metric in active_series:
        if city_keys and city_key not in city_keys:
            continue
        if city_key not in CITY_CONFIG:
            logger.debug(f"No weather config for {city_key} ({series_ticker}) — skipping")
            continue

        city_cfg  = CITY_CONFIG[city_key]
        city_name = city_cfg["name"]
        cursor    = None

        try:
            while True:
                params: dict = {"series_ticker": series_ticker, "status": "open", "limit": 200}
                if cursor:
                    params["cursor"] = cursor

                data = await client.get_markets(params)
                raw_markets = data.get("markets", [])

                for m in raw_markets:
                    ticker = m.get("ticker", "")
                    title  = m.get("title", ticker)
                    report.total_raw += 1

                    # ── Parse ticker ─────────────────────────────────────────
                    if metric == "rain":
                        parsed = _parse_rain_ticker(ticker, city_key, title)
                    else:
                        if "-B" in ticker:
                            report.filtered.append(FilteredMarket(
                                ticker=ticker, city=city_key, title=title,
                                reason="bracket"))
                            continue
                        parsed = _parse_temp_ticker(ticker, city_key, metric, title)

                    if not parsed:
                        report.filtered.append(FilteredMarket(
                            ticker=ticker, city=city_key, title=title,
                            reason="bracket"))
                        continue

                    if parsed["target_date"] < today:
                        report.filtered.append(FilteredMarket(
                            ticker=ticker, city=city_key, title=title,
                            reason="expired"))
                        continue

                    # ── Price resolution ──────────────────────────────────────
                    yes_ask_d = m.get("yes_ask_dollars")
                    no_ask_d  = m.get("no_ask_dollars")
                    last_d    = m.get("last_price_dollars")
                    yes_ask_c = m.get("yes_ask")
                    no_ask_c  = m.get("no_ask")
                    last_c    = m.get("last_price")

                    if yes_ask_d is not None:
                        yes_price = float(yes_ask_d)
                    elif yes_ask_c is not None:
                        yes_price = yes_ask_c / 100.0
                    elif last_d is not None:
                        yes_price = float(last_d)
                    elif last_c is not None:
                        yes_price = last_c / 100.0
                    else:
                        report.filtered.append(FilteredMarket(
                            ticker=ticker, city=city_key, title=title,
                            reason="no_price"))
                        continue

                    if no_ask_d is not None:
                        no_price = float(no_ask_d)
                    elif no_ask_c is not None:
                        no_price = no_ask_c / 100.0
                    else:
                        no_price = 1.0 - yes_price

                    # Near-certain markets — skip silently
                    if yes_price > 0.97 or yes_price < 0.03:
                        continue

                    # ── Liquidity filters ─────────────────────────────────────
                    yes_ask_size = float(m.get("yes_ask_size_fp") or 0)
                    volume_24h   = float(m.get("volume_24h_fp") or 0)

                    if yes_ask_size < MIN_ASK_SIZE:
                        logger.info(
                            f"LIQUIDITY SKIP {ticker}: ask_size={yes_ask_size:.0f} < {MIN_ASK_SIZE}")
                        report.filtered.append(FilteredMarket(
                            ticker=ticker, city=city_key, title=title,
                            reason="low_ask", ask_size=yes_ask_size,
                            volume_24h=volume_24h, yes_price=yes_price))
                        continue

                    if volume_24h < MIN_VOLUME_24H:
                        logger.info(
                            f"LIQUIDITY SKIP {ticker}: volume_24h={volume_24h:.0f} < {MIN_VOLUME_24H}")
                        report.filtered.append(FilteredMarket(
                            ticker=ticker, city=city_key, title=title,
                            reason="low_volume", ask_size=yes_ask_size,
                            volume_24h=volume_24h, yes_price=yes_price))
                        continue

                    report.markets.append(WeatherMarket(
                        slug=ticker, market_id=ticker, platform="kalshi",
                        title=title, city_key=city_key, city_name=city_name,
                        target_date=parsed["target_date"],
                        threshold_f=parsed["threshold_f"],
                        metric=parsed["metric"], direction=parsed["direction"],
                        yes_price=yes_price, no_price=no_price,
                        volume=float(m.get("volume", 0) or 0),
                    ))

                cursor = data.get("cursor")
                if not cursor or not raw_markets:
                    break

        except Exception as e:
            logger.warning(f"Failed to fetch Kalshi markets for {series_ticker}: {e}")

    liq_filtered = [f for f in report.filtered if f.reason in ("low_ask", "low_volume")]
    logger.info(
        f"Found {len(report.markets)} tradeable markets across "
        f"{len({m.city_key for m in report.markets})} cities from "
        f"{report.series_scanned} series "
        f"(raw={report.total_raw}, liquidity_filtered={len(liq_filtered)}, "
        f"brackets={sum(1 for f in report.filtered if f.reason=='bracket')})"
    )
    return report
