"""Weather market dataclass shared across Kalshi fetchers."""
from dataclasses import dataclass
from datetime import date
from typing import Optional


@dataclass
class WeatherMarket:
    """A weather temperature prediction market."""
    slug: str
    market_id: str
    platform: str
    title: str
    city_key: str
    city_name: str
    target_date: date
    threshold_f: float       # Temperature threshold in Fahrenheit
    metric: str              # "high" or "low"
    direction: str           # "above" or "below"
    yes_price: float         # Price of YES outcome (0-1) — used as entry (ask)
    no_price: float          # Price of NO outcome (0-1) — used as entry (ask)
    volume: float = 0.0
    closed: bool = False
    # Bid/ask fields — populated when the API exposes them separately.
    # NOTE: Kalshi's /markets endpoint returns yes_ask and no_ask but does NOT
    # expose yes_bid / no_bid in the bulk listing response.  Individual market
    # endpoints (/markets/{ticker}) may return an orderbook, but that would
    # require one extra API call per market, which is too expensive at scale.
    # When yes_bid == 0 it means bid data is unavailable; fall back to yes_price.
    yes_ask: float = 0.0     # Best ask for YES (filled from API when available)
    yes_bid: float = 0.0     # Best bid for YES (0 = unavailable)

    @property
    def market_direction(self) -> str:
        """Alias for direction — used by weather_signals.py cold-day exception logic."""
        return self.direction
