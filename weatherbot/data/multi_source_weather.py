"""
Multi-source weather data fetcher.

Fetches ensemble forecasts from four independent sources and returns
per-source member arrays for the ensemble-of-ensembles probability model.
"""
import asyncio
import logging
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional

import httpx

from weatherbot.data.weather import CITY_CONFIG, _celsius_to_fahrenheit

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

        # Return the NWS point forecast as a single deterministic member.
        # No synthetic spread — the probability engine uses the Gaussian CDF
        # std floor (3°F for highs, 2°F for lows) to express inherent
        # uncertainty around a single point value.
        highs: List[float] = [day_high] if day_high is not None else []
        lows:  List[float] = [day_low]  if day_low  is not None else []

        if not highs and not lows:
            return SourceForecast(source="nws", member_highs=[], member_lows=[],
                                  ok=False, error="no usable temp values")

        logger.debug(f"NWS {city_key} {target_date}: high={day_high} low={day_low} (deterministic)")
        return SourceForecast(source="nws", member_highs=highs, member_lows=lows)

    except Exception as e:
        logger.warning(f"NWS forecast fetch failed for {city_key}: {e}")
        return SourceForecast(source="nws", member_highs=[], member_lows=[],
                              ok=False, error=str(e))


# ── Rain probability fetcher ──────────────────────────────────────────────────

async def fetch_rain_probability(city_key: str, target_date: date) -> Optional[float]:
    """
    Fetch daily precipitation probability (0-1) from Open-Meteo for a city.
    Returns the mean of the GFS ensemble's precipitation_probability members,
    or None on failure.
    """
    city = CITY_CONFIG.get(city_key)
    if not city:
        return None

    cache_key = f"{city_key}_{target_date.isoformat()}"
    now = time.time()
    if cache_key in _rain_cache:
        cached_time, cached_val = _rain_cache[cache_key]
        if now - cached_time < _SOURCES_CACHE_TTL:
            logger.debug(f"fetch_rain_probability cache hit: {city_key} {target_date}")
            return cached_val

    params = {
        "latitude":   city["lat"],
        "longitude":  city["lon"],
        "daily":      "precipitation_probability_max",
        "start_date": target_date.isoformat(),
        "end_date":   target_date.isoformat(),
        "models":     "gfs_seamless",
    }

    try:
        async with httpx.AsyncClient(timeout=20.0) as client:
            # Use the regular (non-ensemble) forecast API for precipitation
            resp = await client.get(
                "https://api.open-meteo.com/v1/forecast",
                params=params,
            )
            resp.raise_for_status()
            data = resp.json()

        values = data.get("daily", {}).get("precipitation_probability_max", [])
        if not values or values[0] is None:
            return None

        result = float(values[0]) / 100.0   # convert percent to 0-1
        _rain_cache[cache_key] = (now, result)
        return result

    except Exception as e:
        logger.warning(f"Rain probability fetch failed for {city_key}: {e}")
        return None


# ── Real-time NWS observation fetcher ────────────────────────────────────────

async def fetch_current_observation(city_key: str) -> Optional[dict]:
    """
    Fetch the current observed temperature and observed max-so-far today
    from the NWS observations API for the city's NOAA station.

    Returns dict with keys:
        current_temp_f  : most recent observed temperature (°F)
        observed_max_f  : highest temperature observed in the last 24h (°F)
        observed_min_f  : lowest temperature observed in the last 24h (°F)
        obs_time        : ISO timestamp of the most recent observation
    or None on failure.
    """
    city = CITY_CONFIG.get(city_key)
    if not city:
        return None

    station = city.get("noaa_station") or city.get("nws_station")
    if not station:
        return None

    headers = {"User-Agent": "KalshiWeatherArb/1.0 (weather-arb-bot)"}

    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            # Latest single observation
            latest_url = f"https://api.weather.gov/stations/{station}/observations/latest"
            resp = await client.get(latest_url, headers=headers)
            resp.raise_for_status()
            latest_data = resp.json()

            props = latest_data.get("properties", {})
            temp_c = props.get("temperature", {}).get("value")
            if temp_c is None:
                return None
            current_temp_f = _celsius_to_fahrenheit(temp_c)
            obs_time = props.get("timestamp", "")

            # Recent 24h observations to find today's max/min
            obs_url = f"https://api.weather.gov/stations/{station}/observations"
            from datetime import timezone
            now_utc = datetime.utcnow().replace(tzinfo=timezone.utc)
            start_utc = (now_utc - timedelta(hours=24)).strftime("%Y-%m-%dT%H:%M:%SZ")
            resp24 = await client.get(
                obs_url,
                params={"start": start_utc, "limit": 200},
                headers=headers,
            )
            resp24.raise_for_status()
            obs_data = resp24.json()

            temps_f = []
            for feature in obs_data.get("features", []):
                tc = feature.get("properties", {}).get("temperature", {}).get("value")
                if tc is not None:
                    temps_f.append(_celsius_to_fahrenheit(tc))

            if not temps_f:
                temps_f = [current_temp_f]

            result = {
                "current_temp_f": round(current_temp_f, 1),
                "observed_max_f": round(max(temps_f), 1),
                "observed_min_f": round(min(temps_f), 1),
                "obs_time": obs_time,
            }
            logger.debug(
                f"NWS obs {city_key} ({station}): current={current_temp_f:.1f}F "
                f"max24h={result['observed_max_f']:.1f}F min24h={result['observed_min_f']:.1f}F"
            )
            return result

    except Exception as e:
        logger.debug(f"fetch_current_observation failed for {city_key}: {e}")
        return None


# ── Caches: (city_key, date_str) -> (fetch_timestamp, result) ────────────────
# NWP model runs update ~every 6h; 2h TTL ensures we get fresh data at each
# model-run scan while cutting API calls ~100x on the 5-min interval scans.
_sources_cache: Dict[str, tuple] = {}
_rain_cache: Dict[str, tuple] = {}
_SOURCES_CACHE_TTL = 7200  # 2 hours


# ── Public API: fetch all sources in parallel ─────────────────────────────────

async def fetch_all_sources(
    city_key: str,
    target_date: date,
) -> Dict[str, SourceForecast]:
    """
    Fetch forecasts from all four sources in parallel.
    Returns dict keyed by source name. Missing/failed sources are included
    with ok=False so callers can log and skip gracefully.

    Results are cached for _SOURCES_CACHE_TTL seconds. Multiple threshold
    markets for the same city+date share the same weather data, so the first
    call fetches and all subsequent calls within the TTL window are free.
    """
    cache_key = f"{city_key}_{target_date.isoformat()}"
    now = time.time()
    if cache_key in _sources_cache:
        cached_time, cached_sources = _sources_cache[cache_key]
        if now - cached_time < _SOURCES_CACHE_TTL:
            logger.debug(f"fetch_all_sources cache hit: {city_key} {target_date}")
            return cached_sources

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
    _sources_cache[cache_key] = (now, sources)
    return sources
