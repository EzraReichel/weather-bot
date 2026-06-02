"""
WeatherDay — the point-in-time world-state the shared ``decide()`` core consumes.

One instance describes a single (city, target_date) market as seen at a specific
``as_of`` timestamp. The backtest archive loader (archive.py) builds these from
the bt_* tables; the same shape could be built from live fetches if the live
path is ever wired to call the shared core.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, Optional

from weatherbot.data.multi_source_weather import SourceForecast
from weatherbot.data.weather_markets import WeatherMarket


@dataclass
class WeatherDay:
    """Everything ``decide()`` needs about one market at one moment in time."""

    market: WeatherMarket
    as_of: datetime                       # tz-aware decision-time clock

    # RAW per-source ensembles (both member_highs and member_lows populated);
    # decide() does the metric remap + grid-bias itself.
    sources: Dict[str, SourceForecast] = field(default_factory=dict)

    # Latest observation, or None. Shape: {current_temp_f, observed_max_f,
    # observed_min_f, obs_time}.
    observation: Optional[dict] = None

    # 30-yr monthly normal for this city/metric (None if unknown).
    climatology_normal: Optional[float] = None

    # Additive grid→station bias for this metric (cities.json grid_bias.{high|low}).
    grid_bias: float = 0.0

    # Precip probability for rain markets (None for temperature markets).
    rain_prob: Optional[float] = None
