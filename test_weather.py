#!/usr/bin/env python3
"""Test Open-Meteo ensemble data pipeline and probability engine."""
import asyncio
import sys
from datetime import date, timedelta

from dotenv import load_dotenv
load_dotenv()

from backend.data.weather import fetch_ensemble_forecast
from backend.core.probability import compute_probability


async def main():
    print("─" * 50)
    print("TEST: Weather Data Pipeline")
    print("─" * 50)

    # ── 1. Fetch ensemble forecast ────────────────────────────────
    print("\nFetching GFS ensemble for NYC (tomorrow)...")
    target = date.today() + timedelta(days=1)
    forecast = None
    try:
        forecast = await fetch_ensemble_forecast("nyc", target)
    except Exception as e:
        print(f"❌ fetch_ensemble_forecast raised: {type(e).__name__}: {e}")
        return False

    if forecast is None:
        print("❌ Got None — Open-Meteo returned no data for NYC tomorrow")
        return False

    print(f"✅ Ensemble received")
    print(f"   Members:        {forecast.num_members}")
    print(f"   High mean/std:  {forecast.mean_high:.1f}°F ± {forecast.std_high:.1f}°F")
    print(f"   Low  mean/std:  {forecast.mean_low:.1f}°F  ± {forecast.std_low:.1f}°F")
    print(f"   Target date:    {forecast.target_date}")

    if forecast.num_members < 5:
        print(f"⚠️  Only {forecast.num_members} members — expected ~31")

    # ── 2. Probability engine ─────────────────────────────────────
    print("\nRunning probability engine on high temps...")
    threshold = round(forecast.mean_high)  # pick a threshold near the mean so both methods give ~50%
    print(f"   Threshold: {threshold}°F  (≈ ensemble mean)")

    result = compute_probability(
        member_values=forecast.member_highs,
        threshold_f=float(threshold),
        direction="above",
        target_date=target,
    )

    if result is None:
        print("❌ compute_probability returned None")
        return False

    print(f"   Gaussian CDF P(high > {threshold}°F): {result.model_prob:.1%}")
    print(f"   Raw ensemble fraction:                {result.ensemble_fraction:.1%}")
    print(f"   Lead-time factor:                     {result.lead_time_factor}×")
    print(f"   Adjusted std:                         {result.adjusted_std:.1f}°F")
    print(f"   Confidence:                           {result.confidence:.0%}")
    print(f"   Low-confidence flag:                  {result.low_confidence_flag}")

    # Sanity: near-mean threshold → both methods should give ~40–60%
    diff = abs(result.model_prob - result.ensemble_fraction)
    print(f"\n   CDF vs fraction difference: {diff:.1%}", end="  ")
    if diff > 0.15:
        print("⚠️  (>15%, low-confidence flag would trigger)")
    else:
        print("✅")

    # ── 3. Edge case: std = 0 ─────────────────────────────────────
    print("\nEdge case: ensemble std = 0 (all members agree)...")
    try:
        r2 = compute_probability(
            member_values=[72.0] * 31,
            threshold_f=75.0,
            direction="above",
            target_date=target,
        )
        print(f"✅ Handled cleanly — P(high > 75°F | all=72°F): {r2.model_prob:.1%}")
    except Exception as e:
        print(f"❌ Crashed: {e}")
        return False

    # ── 4. Edge case: Open-Meteo unavailable (bad city key) ───────
    print("\nEdge case: unknown city key...")
    try:
        bad = await fetch_ensemble_forecast("atlantis", target)
        if bad is None:
            print("✅ Returns None gracefully for unknown city")
        else:
            print("⚠️  Returned data for unknown city (unexpected)")
    except Exception as e:
        print(f"❌ Raised instead of returning None: {e}")
        return False

    print(f"\n✅ Weather pipeline working — {forecast.num_members} members, "
          f"NYC high {forecast.mean_high:.1f}°F ± {forecast.std_high:.1f}°F")
    return True


if __name__ == "__main__":
    ok = asyncio.run(main())
    sys.exit(0 if ok else 1)
