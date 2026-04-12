"""Weather data fetcher using Open-Meteo Ensemble API and NWS observations."""
import httpx
import logging
from dataclasses import dataclass, field
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional
import statistics
import time

logger = logging.getLogger("trading_bot")

# City configurations with lat/lon and NWS station identifiers
CITY_CONFIG: Dict[str, dict] = {
    # Coordinates match the exact NOAA/NWS station Kalshi uses for settlement.
    # Using airport station coords (not city center) is critical — e.g. downtown LA
    # runs 5-8°F hotter than LAX, which would produce massive phantom edges.
    "nyc": {
        "name": "New York City",
        "lat": 40.7128,   # KNYC — Central Park
        "lon": -74.0060,
        "nws_station": "KNYC",
        "nws_office": "OKX",
        "nws_gridpoint": "OKX/33,37",
    },
    "chicago": {
        "name": "Chicago",
        "lat": 41.9742,   # KORD — O'Hare
        "lon": -87.9073,
        "nws_station": "KORD",
        "nws_office": "LOT",
        "nws_gridpoint": "LOT/75,72",
    },
    "miami": {
        "name": "Miami",
        "lat": 25.7959,   # KMIA — Miami Intl
        "lon": -80.2870,
        "nws_station": "KMIA",
        "nws_office": "MFL",
        "nws_gridpoint": "MFL/75,53",
    },
    "los_angeles": {
        "name": "Los Angeles",
        "lat": 33.9425,   # KLAX — LAX airport
        "lon": -118.4081,
        "nws_station": "KLAX",
        "nws_office": "LOX",
        "nws_gridpoint": "LOX/154,44",
    },
    "denver": {
        "name": "Denver",
        "lat": 39.8561,   # KDEN — Denver Intl
        "lon": -104.6737,
        "nws_station": "KDEN",
        "nws_office": "BOU",
        "nws_gridpoint": "BOU/62,60",
    },
    "austin": {
        "name": "Austin",
        "lat": 30.1975,   # KAUS — Austin-Bergstrom Intl
        "lon": -97.6664,
        "nws_station": "KAUS",
        "nws_office": "EWX",
        "nws_gridpoint": "EWX/157,90",
    },
    "houston": {
        "name": "Houston",
        "lat": 29.9844,   # KIAH — George Bush Intercontinental
        "lon": -95.3414,
        "nws_station": "KIAH",
        "nws_office": "HGX",
        "nws_gridpoint": "HGX/65,99",
    },
    "boston": {
        "name": "Boston",
        "lat": 42.3606,   # KBOS — Logan Intl
        "lon": -71.0106,
        "nws_station": "KBOS",
        "nws_office": "BOX",
        "nws_gridpoint": "BOX/69,82",
    },
    "washington_dc": {
        "name": "Washington DC",
        "lat": 38.9531,   # KDCA — Reagan National
        "lon": -77.4565,
        "nws_station": "KDCA",
        "nws_office": "LWX",
        "nws_gridpoint": "LWX/97,70",
    },
    "phoenix": {
        "name": "Phoenix",
        "lat": 33.4373,   # KPHX — Phoenix Sky Harbor
        "lon": -112.0078,
        "nws_station": "KPHX",
        "nws_office": "PSR",
        "nws_gridpoint": "PSR/158,57",
    },
    "seattle": {
        "name": "Seattle",
        "lat": 47.4502,   # KSEA — Seattle-Tacoma Intl
        "lon": -122.3088,
        "nws_station": "KSEA",
        "nws_office": "SEW",
        "nws_gridpoint": "SEW/124,68",
    },
    "san_francisco": {
        "name": "San Francisco",
        "lat": 37.6213,   # KSFO — SFO airport
        "lon": -122.3790,
        "nws_station": "KSFO",
        "nws_office": "MTR",
        "nws_gridpoint": "MTR/84,82",
    },
    "atlanta": {
        "name": "Atlanta",
        "lat": 33.6407,   # KATL — Hartsfield-Jackson
        "lon": -84.4277,
        "nws_station": "KATL",
        "nws_office": "FFC",
        "nws_gridpoint": "FFC/52,57",
    },
    "dallas": {
        "name": "Dallas",
        "lat": 32.8998,   # KDFW — DFW airport
        "lon": -97.0403,
        "nws_station": "KDFW",
        "nws_office": "FWD",
        "nws_gridpoint": "FWD/99,80",
    },
    "las_vegas": {
        "name": "Las Vegas",
        "lat": 36.0840,   # KLAS — McCarran/Harry Reid Intl
        "lon": -115.1537,
        "nws_station": "KLAS",
        "nws_office": "VEF",
        "nws_gridpoint": "VEF/112,77",
    },
    "minneapolis": {
        "name": "Minneapolis",
        "lat": 44.8820,   # KMSP — Minneapolis-Saint Paul Intl
        "lon": -93.2218,
        "nws_station": "KMSP",
        "nws_office": "MPX",
        "nws_gridpoint": "MPX/107,70",
    },
    "new_orleans": {
        "name": "New Orleans",
        "lat": 29.9934,   # KMSY — Louis Armstrong Intl
        "lon": -90.2580,
        "nws_station": "KMSY",
        "nws_office": "LIX",
        "nws_gridpoint": "LIX/67,62",
    },
    "oklahoma_city": {
        "name": "Oklahoma City",
        "lat": 35.3931,   # KOKC — Will Rogers World Airport
        "lon": -97.6007,
        "nws_station": "KOKC",
        "nws_office": "OUN",
        "nws_gridpoint": "OUN/103,83",
    },
    "san_antonio": {
        "name": "San Antonio",
        "lat": 29.5337,   # KSAT — San Antonio Intl
        "lon": -98.4698,
        "nws_station": "KSAT",
        "nws_office": "EWX",
        "nws_gridpoint": "EWX/126,74",
    },
    "philadelphia": {
        "name": "Philadelphia",
        "lat": 39.8721,   # KPHL — Philadelphia Intl
        "lon": -75.2411,
        "nws_station": "KPHL",
        "nws_office": "PHI",
        "nws_gridpoint": "PHI/49,69",
    },
}


@dataclass
class EnsembleForecast:
    """Ensemble weather forecast with per-member data."""
    city_key: str
    city_name: str
    target_date: date
    member_highs: List[float]  # Daily max temps (F) per ensemble member
    member_lows: List[float]   # Daily min temps (F) per ensemble member
    mean_high: float = 0.0
    std_high: float = 0.0
    mean_low: float = 0.0
    std_low: float = 0.0
    num_members: int = 0
    fetched_at: datetime = field(default_factory=datetime.utcnow)

    def __post_init__(self):
        if self.member_highs:
            self.mean_high = statistics.mean(self.member_highs)
            self.std_high = statistics.stdev(self.member_highs) if len(self.member_highs) > 1 else 0.0
            self.num_members = len(self.member_highs)
        if self.member_lows:
            self.mean_low = statistics.mean(self.member_lows)
            self.std_low = statistics.stdev(self.member_lows) if len(self.member_lows) > 1 else 0.0

    def probability_high_above(self, threshold_f: float) -> float:
        """Fraction of ensemble members with daily high above threshold."""
        if not self.member_highs:
            return 0.5
        count = sum(1 for h in self.member_highs if h > threshold_f)
        return count / len(self.member_highs)

    def probability_high_below(self, threshold_f: float) -> float:
        """Fraction of ensemble members with daily high below threshold."""
        return 1.0 - self.probability_high_above(threshold_f)

    def probability_low_above(self, threshold_f: float) -> float:
        """Fraction of ensemble members with daily low above threshold."""
        if not self.member_lows:
            return 0.5
        count = sum(1 for l in self.member_lows if l > threshold_f)
        return count / len(self.member_lows)

    def probability_low_below(self, threshold_f: float) -> float:
        """Fraction of ensemble members with daily low below threshold."""
        return 1.0 - self.probability_low_above(threshold_f)

    @property
    def ensemble_agreement(self) -> float:
        """How one-sided the ensemble is (0.5 = split, 1.0 = unanimous)."""
        if not self.member_highs:
            return 0.5
        median = statistics.median(self.member_highs)
        above = sum(1 for h in self.member_highs if h > median)
        frac = above / len(self.member_highs)
        return max(frac, 1 - frac)


# Simple cache: (city_key, target_date_str) -> (timestamp, EnsembleForecast)
_forecast_cache: Dict[str, tuple] = {}
_CACHE_TTL = 900  # 15 minutes


def _celsius_to_fahrenheit(c: float) -> float:
    return c * 9.0 / 5.0 + 32.0


async def fetch_ensemble_forecast(city_key: str, target_date: Optional[date] = None) -> Optional[EnsembleForecast]:
    """
    Fetch ensemble forecast from Open-Meteo Ensemble API (free, 31-member GFS).
    Returns per-member daily max/min temperatures in Fahrenheit.
    """
    if city_key not in CITY_CONFIG:
        logger.warning(f"Unknown city key: {city_key}")
        return None

    if target_date is None:
        target_date = date.today()

    cache_key = f"{city_key}_{target_date.isoformat()}"
    now = time.time()
    if cache_key in _forecast_cache:
        cached_time, cached_forecast = _forecast_cache[cache_key]
        if now - cached_time < _CACHE_TTL:
            return cached_forecast

    city = CITY_CONFIG[city_key]

    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            # Open-Meteo Ensemble API — GFS ensemble with 31 members
            params = {
                "latitude": city["lat"],
                "longitude": city["lon"],
                "daily": "temperature_2m_max,temperature_2m_min",
                "temperature_unit": "fahrenheit",
                "start_date": target_date.isoformat(),
                "end_date": target_date.isoformat(),
                "models": "gfs_seamless",
            }

            response = await client.get(
                "https://ensemble-api.open-meteo.com/v1/ensemble",
                params=params,
            )
            response.raise_for_status()
            data = response.json()

            daily = data.get("daily", {})

            # Open-Meteo returns each ensemble member as a separate key:
            #   temperature_2m_max (control), temperature_2m_max_member01, ..., _member30
            # Collect all member values for highs and lows
            member_highs = []
            member_lows = []

            for key, values in daily.items():
                if not isinstance(values, list) or not values:
                    continue
                val = values[0]
                if val is None:
                    continue
                if "temperature_2m_max" in key:
                    member_highs.append(float(val))
                elif "temperature_2m_min" in key:
                    member_lows.append(float(val))

            if not member_highs:
                logger.warning(f"No ensemble data for {city_key} on {target_date}")
                return None

            forecast = EnsembleForecast(
                city_key=city_key,
                city_name=city["name"],
                target_date=target_date,
                member_highs=member_highs,
                member_lows=member_lows,
            )

            _forecast_cache[cache_key] = (now, forecast)
            logger.info(f"Ensemble forecast for {city['name']} on {target_date}: "
                        f"High {forecast.mean_high:.1f}F +/- {forecast.std_high:.1f}F "
                        f"({forecast.num_members} members)")

            return forecast

    except Exception as e:
        logger.warning(f"Failed to fetch ensemble forecast for {city_key}: {e}")
        return None


async def fetch_nws_observed_temperature(city_key: str, target_date: Optional[date] = None) -> Optional[Dict[str, float]]:
    """
    Fetch observed temperature from NWS API for settlement.
    Returns dict with 'high' and 'low' in Fahrenheit, or None if not available.
    """
    if city_key not in CITY_CONFIG:
        return None

    city = CITY_CONFIG[city_key]
    if target_date is None:
        target_date = date.today()

    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            # NWS observations endpoint
            station = city["nws_station"]
            url = f"https://api.weather.gov/stations/{station}/observations"
            headers = {"User-Agent": "(trading-bot, contact@example.com)"}

            # Get observations for the full local calendar day.
            # NWS stations report in local time; Kalshi settles on local calendar day.
            # Use ET (UTC-5/UTC-4) local midnight → next midnight expressed in UTC.
            from zoneinfo import ZoneInfo
            et = ZoneInfo("America/New_York")
            local_start = datetime(target_date.year, target_date.month, target_date.day,
                                   0, 0, 0, tzinfo=et)
            local_end   = local_start + timedelta(days=1)
            start = local_start.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            end   = local_end.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

            response = await client.get(url, params={"start": start, "end": end}, headers=headers)
            response.raise_for_status()
            data = response.json()

            features = data.get("features", [])
            if not features:
                return None

            temps = []
            for obs in features:
                props = obs.get("properties", {})
                temp_c = props.get("temperature", {}).get("value")
                if temp_c is not None:
                    temps.append(_celsius_to_fahrenheit(temp_c))

            if not temps:
                return None

            return {
                "high": max(temps),
                "low": min(temps),
            }

    except Exception as e:
        logger.warning(f"Failed to fetch NWS observations for {city_key}: {e}")
        return None
