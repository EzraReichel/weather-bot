#!/usr/bin/env python3
"""
Test weather data pipeline, cities.json integration, and probability engine.
Run: python tests/test_weather.py
"""
import asyncio
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from datetime import date, timedelta
from dotenv import load_dotenv
load_dotenv()

SEP = "─" * 52


def check(label: str, result: bool):
    icon = "✅" if result else "❌"
    print(f"  {icon} {label}")
    return result


async def main():
    all_ok = True

    # ── 1. cities.json integration ────────────────────────────────
    print(SEP)
    print("1. CITIES.JSON")
    print(SEP)

    from weatherbot.data.weather import CITY_CONFIG, get_climatology_normal

    enabled = {k: v for k, v in CITY_CONFIG.items() if v.get("enabled")}
    disabled = {k: v for k, v in CITY_CONFIG.items() if not v.get("enabled")}

    all_ok &= check(f"CITY_CONFIG loaded ({len(CITY_CONFIG)} total cities)", len(CITY_CONFIG) > 0)
    all_ok &= check(f"Enabled cities: {len(enabled)}", len(enabled) >= 18)
    all_ok &= check(f"Miami disabled", not CITY_CONFIG.get("miami", {}).get("enabled", True))
    all_ok &= check(f"LA disabled", not CITY_CONFIG.get("los_angeles", {}).get("enabled", True))

    # Verify required fields in each enabled city
    required = {"name", "lat", "lon", "nws_station", "normals", "kalshi_series"}
    missing_fields = [
        k for k, v in enabled.items()
        if not required.issubset(v.keys())
    ]
    all_ok &= check(
        f"All enabled cities have required fields {required}",
        len(missing_fields) == 0,
    )
    if missing_fields:
        print(f"    Missing fields in: {missing_fields}")

    # Verify normals shape: 12 months × [high, low]
    bad_normals = [
        k for k, v in enabled.items()
        if len(v.get("normals", [])) != 12 or any(len(m) != 2 for m in v["normals"])
    ]
    all_ok &= check(
        "All enabled cities have 12 months of [high, low] normals",
        len(bad_normals) == 0,
    )
    if bad_normals:
        print(f"    Bad normals in: {bad_normals}")

    # Test get_climatology_normal
    today = date.today()
    nyc_high = get_climatology_normal("nyc", today, "high")
    nyc_low  = get_climatology_normal("nyc", today, "low")
    all_ok &= check(
        f"get_climatology_normal('nyc', today, 'high') = {nyc_high}°F",
        isinstance(nyc_high, (int, float)) and 20 < nyc_high < 100,
    )
    all_ok &= check(
        f"get_climatology_normal('nyc', today, 'low') = {nyc_low}°F",
        isinstance(nyc_low, (int, float)) and nyc_low < nyc_high,
    )
    all_ok &= check(
        "Unknown city returns None",
        get_climatology_normal("atlantis", today, "high") is None,
    )

    # ── 2. KNOWN_SERIES_MAP built from cities.json ─────────────────
    print()
    print(SEP)
    print("2. KNOWN_SERIES_MAP (built from cities.json)")
    print(SEP)

    from weatherbot.data.kalshi_markets import KNOWN_SERIES_MAP

    all_ok &= check(f"KNOWN_SERIES_MAP has entries ({len(KNOWN_SERIES_MAP)})", len(KNOWN_SERIES_MAP) > 0)
    # NYC high series should be present
    nyc_high_series = [k for k, v in KNOWN_SERIES_MAP.items() if v == ("nyc", "high")]
    all_ok &= check(
        f"NYC high series present: {nyc_high_series}",
        len(nyc_high_series) > 0,
    )
    # Every value should be a (city_key, metric) tuple with valid city and metric
    valid_metrics = {"high", "low", "rain"}
    bad_entries = [
        (k, v) for k, v in KNOWN_SERIES_MAP.items()
        if v[0] not in CITY_CONFIG or v[1] not in valid_metrics
    ]
    all_ok &= check(
        "All series map to valid (city_key, metric) pairs",
        len(bad_entries) == 0,
    )
    if bad_entries:
        print(f"    Bad entries: {bad_entries[:5]}")

    # ── 3. Ensemble forecast ───────────────────────────────────────
    print()
    print(SEP)
    print("3. ENSEMBLE FORECAST (Open-Meteo)")
    print(SEP)

    from weatherbot.data.weather import fetch_ensemble_forecast

    target = date.today() + timedelta(days=1)
    print(f"  Fetching NYC forecast for {target}...")
    try:
        forecast = await fetch_ensemble_forecast("nyc", target)
    except Exception as e:
        all_ok &= check(f"fetch_ensemble_forecast raised: {e}", False)
        forecast = None

    if forecast is not None:
        all_ok &= check(f"Got {forecast.num_members} ensemble members", forecast.num_members >= 5)
        all_ok &= check(
            f"High mean {forecast.mean_high:.1f}°F in plausible range (0–120°F)",
            0 < forecast.mean_high < 120,
        )
        all_ok &= check(
            f"Std > 0: std_high={forecast.std_high:.1f}°F",
            forecast.std_high > 0,
        )
        print(f"  High: {forecast.mean_high:.1f}°F ± {forecast.std_high:.1f}°F")
        print(f"  Low:  {forecast.mean_low:.1f}°F  ± {forecast.std_low:.1f}°F")
    else:
        all_ok &= check("fetch_ensemble_forecast returned None for NYC", False)

    # Unknown city returns None gracefully
    try:
        bad = await fetch_ensemble_forecast("atlantis", target)
        all_ok &= check("Unknown city returns None (no crash)", bad is None)
    except Exception as e:
        all_ok &= check(f"Unknown city raised instead of returning None: {e}", False)

    # ── 4. Probability engine ──────────────────────────────────────
    print()
    print(SEP)
    print("4. PROBABILITY ENGINE")
    print(SEP)

    from weatherbot.core.probability import compute_probability

    if forecast is not None:
        threshold = round(forecast.mean_high)
        result = compute_probability(
            member_values=forecast.member_highs,
            threshold_f=float(threshold),
            direction="above",
            target_date=target,
        )
        all_ok &= check("compute_probability returns a result", result is not None)
        if result:
            all_ok &= check(
                f"P(high > {threshold}°F) in [0.05, 0.95]: {result.model_prob:.1%}",
                0.05 <= result.model_prob <= 0.95,
            )
            all_ok &= check(
                f"Ensemble fraction in [0, 1]: {result.ensemble_fraction:.1%}",
                0.0 <= result.ensemble_fraction <= 1.0,
            )
            print(f"  model_prob={result.model_prob:.1%}  ensemble_fraction={result.ensemble_fraction:.1%}  "
                  f"lead_factor={result.lead_time_factor}×  confidence={result.confidence:.0%}")

    # Edge case: zero std (all members agree)
    r_zero = compute_probability(
        member_values=[72.0] * 31,
        threshold_f=75.0,
        direction="above",
        target_date=target,
    )
    all_ok &= check(
        f"Zero-std edge case handled — P={r_zero.model_prob:.1%} (expected ~0.05)",
        r_zero is not None,
    )

    # Edge case: above direction should give ~50% when threshold == mean
    r_mid = compute_probability(
        member_values=[70.0] * 16 + [75.0] * 15,
        threshold_f=72.0,
        direction="above",
        target_date=target,
    )
    all_ok &= check(
        f"Mid-split ensemble P(above)={r_mid.model_prob:.1%} in [0.3, 0.7]",
        0.3 <= r_mid.model_prob <= 0.7,
    )

    # ── 5. NWS observed temperature ───────────────────────────────
    print()
    print(SEP)
    print("5. NWS OBSERVED TEMPERATURE (settlement data)")
    print(SEP)

    from weatherbot.data.weather import fetch_nws_observed_temperature

    yesterday = date.today() - timedelta(days=1)
    print(f"  Fetching NYC observed temps for {yesterday}...")
    try:
        obs = await fetch_nws_observed_temperature("nyc", yesterday)
        if obs:
            all_ok &= check(
                f"NWS data returned — high={obs.get('high')}°F  low={obs.get('low')}°F",
                obs.get("high") is not None,
            )
        else:
            print("  ⚠️  No NWS data for yesterday — this is OK if data isn't posted yet")
            print("     (Settlement will skip trades when NWS data is unavailable)")
    except Exception as e:
        all_ok &= check(f"fetch_nws_observed_temperature raised: {e}", False)

    print()
    print(SEP)
    if all_ok:
        print("✅ All weather pipeline tests passed")
    else:
        print("❌ Some tests failed — see above")
    return all_ok


if __name__ == "__main__":
    ok = asyncio.run(main())
    sys.exit(0 if ok else 1)
