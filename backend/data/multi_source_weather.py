"""
Multi-source weather data fetcher.

Fetches ensemble forecasts from four independent sources and returns
per-source member arrays for the ensemble-of-ensembles probability model.
"""
import asyncio
import logging
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional

import httpx

from backend.data.weather import CITY_CONFIG, _celsius_to_fahrenheit

logger = logging.getLogger("weatherbot")


# ── Source result dataclass ───────────────────────────────────────────────────

@dataclass
class SourceForecast:
    """Temperature forecast from one data source."""
    source: str                   # "gfs", "ecmwf", "gem", "nws"
    member_highs: List[float]     # ensemble member high temps (°F)
    member_lows: List[float]      # ensemble member low temps (°F)
    num_members: int = 0
    ok: bool = True
    error: str = ""

    def __post_init__(self):
        self.num_members = len(self.member_highs)


# ── Open-Meteo ensemble fetcher (shared for GFS / ECMWF / GEM) ───────────────

async def _fetch_open_meteo_ensemble(
    city_key: str,
    target_date: date,
    model: str,
) -> Optional[SourceForecast]:
    """
    Fetch ensemble forecast from Open-Meteo for any supported ensemble model.
    Returns per-member daily high/low in °F.
    """
    city = CITY_CONFIG.get(city_key)
    if not city:
        return None

    params = {
        "latitude":         city["lat"],
        "longitude":        city["lon"],
        "daily":            "temperature_2m_max,temperature_2m_min",
        "temperature_unit": "fahrenheit",
        "start_date":       target_date.isoformat(),
        "end_date":         target_date.isoformat(),
        "models":           model,
    }

    try:
        async with httpx.AsyncClient(timeout=20.0) as client:
            resp = await client.get(
                "https://ensemble-api.open-meteo.com/v1/ensemble",
                params=params,
            )
            resp.raise_for_status()
            data = resp.json()

        daily = data.get("daily", {})
        highs, lows = [], []

        for key, values in daily.items():
            if "temperature_2m_max" not in key and "temperature_2m_min" not in key:
                continue
            if not isinstance(values, list) or not values or values[0] is None:
                continue
            val = float(values[0])
            if "temperature_2m_max" in key:
                highs.append(val)
            elif "temperature_2m_min" in key:
                lows.append(val)

        if not highs:
            return SourceForecast(source=model, member_highs=[], member_lows=[],
                                  ok=False, error="no data returned")

        return SourceForecast(source=model, member_highs=highs, member_lows=lows)

    except Exception as e:
        logger.warning(f"Open-Meteo {model} fetch failed for {city_key}: {e}")
        return SourceForecast(source=model, member_highs=[], member_lows=[],
                              ok=False, error=str(e))


# ── NWS point forecast fetcher ────────────────────────────────────────────────

async def _fetch_nws_point_forecast(
    city_key: str,
    target_date: date,
) -> Optional[SourceForecast]:
    """
    Fetch NWS gridpoint forecast for a city.
    Returns synthetic members built around the forecast high/low with ±3°F (next-day)
    or ±5°F (2+ day) fixed uncertainty, giving us a pseudo-ensemble for the CDF calc.
    """
    city = CITY_CONFIG.get(city_key)
    if not city:
        return None

    gridpoint = city.get("nws_gridpoint")  # e.g. "OKX/33,37"
    if not gridpoint:
        return None

    headers = {"User-Agent": "KalshiWeatherArb/1.0 (weather-arb-bot)"}

    try:
        async with httpx.AsyncClient(timeout=20.0) as client:
            url = f"https://api.weather.gov/gridpoints/{gridpoint}/forecast"
            resp = await client.get(url, headers=headers)
            resp.raise_for_status()
            data = resp.json()

        periods = data.get("properties", {}).get("periods", [])
        if not periods:
            return SourceForecast(source="nws", member_highs=[], member_lows=[],
                                  ok=False, error="no forecast periods")

        # Find the forecast period(s) for target_date
        target_str = target_date.isoformat()
        day_high: Optional[float] = None
        day_low:  Optional[float] = None

        for period in periods:
            start = period.get("startTime", "")
            if target_str not in start:
                continue
            temp_f = period.get("temperature")
            if temp_f is None:
                continue
            # NWS uses °F by default for US gridpoints
            unit = period.get("temperatureUnit", "F")
            if unit == "C":
                temp_f = _celsius_to_fahrenheit(float(temp_f))
            else:
                temp_f = float(temp_f)

            if period.get("isDaytime", True):
                day_high = temp_f
            else:
                day_low = temp_f

        if day_high is None and day_low is None:
            return SourceForecast(source="nws", member_highs=[], member_lows=[],
                                  ok=False, error=f"no periods matched {target_str}")

        # Fixed uncertainty: ±3°F next day, ±5°F 2+ days out
        days_out = (target_date - date.today()).days
        sigma = 3.0 if days_out <= 1 else 5.0

        # Build synthetic 21-member pseudo-ensemble from the NWS point value
        import numpy as np
        rng = np.random.default_rng(seed=int(target_date.strftime("%Y%m%d")))

        highs: List[float] = []
        lows:  List[float] = []

        if day_high is not None:
            highs = [float(v) for v in rng.normal(day_high, sigma, 21)]
        if day_low is not None:
            lows  = [float(v) for v in rng.normal(day_low, sigma, 21)]

        if not highs and not lows:
            return SourceForecast(source="nws", member_highs=[], member_lows=[],
                                  ok=False, error="no usable temp values")

        logger.debug(
            f"NWS {city_key} {target_date}: high={day_high} low={day_low} "
            f"sigma={sigma}°F days_out={days_out}"
        )
        return SourceForecast(source="nws", member_highs=highs, member_lows=lows)

    except Exception as e:
        logger.warning(f"NWS forecast fetch failed for {city_key}: {e}")
        return SourceForecast(source="nws", member_highs=[], member_lows=[],
                              ok=False, error=str(e))


# ── Public API: fetch all sources in parallel ─────────────────────────────────

async def fetch_all_sources(
    city_key: str,
    target_date: date,
) -> Dict[str, SourceForecast]:
    """
    Fetch forecasts from all four sources in parallel.
    Returns dict keyed by source name. Missing/failed sources are included
    with ok=False so callers can log and skip gracefully.
    """
    gfs_task   = _fetch_open_meteo_ensemble(city_key, target_date, "gfs_seamless")
    ecmwf_task = _fetch_open_meteo_ensemble(city_key, target_date, "ecmwf_ifs025")
    gem_task   = _fetch_open_meteo_ensemble(city_key, target_date, "gem_global")
    nws_task   = _fetch_nws_point_forecast(city_key, target_date)

    results = await asyncio.gather(
        gfs_task, ecmwf_task, gem_task, nws_task,
        return_exceptions=True,
    )

    sources: Dict[str, SourceForecast] = {}
    names = ["gfs", "ecmwf", "gem", "nws"]

    for name, result in zip(names, results):
        if isinstance(result, Exception):
            logger.warning(f"Source {name} raised exception for {city_key}: {result}")
            sources[name] = SourceForecast(source=name, member_highs=[], member_lows=[],
                                           ok=False, error=str(result))
        elif result is None:
            sources[name] = SourceForecast(source=name, member_highs=[], member_lows=[],
                                           ok=False, error="returned None")
        else:
            result.source = name   # normalise (GFS was stored as "gfs_seamless" key)
            sources[name] = result

    ok_count = sum(1 for s in sources.values() if s.ok)
    logger.info(
        f"Multi-source fetch {city_key} {target_date}: "
        f"{ok_count}/4 sources OK  "
        + "  ".join(f"{n}={'OK' if sources[n].ok else 'FAIL'}" for n in names)
    )
    return sources
