#!/usr/bin/env python3
"""
Disciplined calibration study over the bt_* archive.

Unlike scripts/calibrate.py (which re-fetches *current* forecasts for *past*
dates — a look-ahead leak), this replays the forecasts AS THEY WERE CAPTURED at
decision time and scores them against the realized outcome.

Everything is scored by leave-one-day-out cross-validated Brier — NOT P&L —
because ~15 days of single-season data makes P&L tuning an overfitting trap.

Stages:
  1. Baseline    — live_default params, no bias.
  2. + Bias      — per-(city,metric) bias correction, LODO cross-validated.
  3. Coarse sweep — std_floor_high/low and the ensemble/CDF blend weight, each
                    config scored with LODO bias. A few knobs, coarse grid.

Usage:
    python scripts/calibrate_archive.py [--start YYYY-MM-DD] [--end YYYY-MM-DD]
                                        [--lead-hours 24] [--out calibration_archive.json]
"""
import argparse
import json
import sys
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(dotenv_path=str(Path(__file__).resolve().parent.parent / ".env"))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from weatherbot.backtest.calibration import (
    load_events, estimate_bias, lodo_cv_predictions, brier, reliability_curve,
)
from weatherbot.core.probability import SOURCE_WEIGHTS
from weatherbot.core.strategy import StrategyParams
from weatherbot.models.weather_db import SessionLocal


def _fmt_rel(curve):
    lines = ["    bucket      model  actual   n"]
    for r in curve:
        lines.append(f"    {r['bucket']:10s} {r['model_mean']:5.2f}  {r['actual_freq']:5.2f}  {r['n']:3d}")
    return "\n".join(lines)


def main():
    ap = argparse.ArgumentParser(description="Disciplined archive calibration study.")
    ap.add_argument("--start", default=None)
    ap.add_argument("--end", default=None)
    ap.add_argument("--lead-hours", type=float, default=24.0,
                    help="forecast lead time to evaluate at (default ~day-ahead)")
    ap.add_argument("--min-samples", type=int, default=5,
                    help="min training days per city/metric to trust a bias")
    ap.add_argument("--out", default="calibration_archive.json")
    args = ap.parse_args()

    # Base params: deterministic static source weights (no live Brier-weight DB
    # lookup, which would leak the live accuracy table into the backtest).
    base = StrategyParams.live_default().with_(use_dynamic_brier_weights=False)

    db = SessionLocal()
    try:
        events = load_events(db, start=args.start, end=args.end, lead_hours_target=args.lead_hours)
    finally:
        db.close()

    if not events:
        print("No events loaded — archive empty for the given window.", file=sys.stderr)
        sys.exit(1)

    days = sorted({e.target_date for e in events})
    leads = sorted(e.lead_hours for e in events)
    print("=" * 68)
    print("  ARCHIVE CALIBRATION STUDY")
    print("=" * 68)
    print(f"  events            : {len(events)}  ({sum(1 for e in events if e.metric=='high')} high, "
          f"{sum(1 for e in events if e.metric=='low')} low)")
    print(f"  span              : {days[0]} → {days[-1]}  ({len(days)} days)")
    print(f"  cities            : {len({e.city_key for e in events})}")
    print(f"  lead hours        : median {leads[len(leads)//2]:.1f}h  "
          f"(min {leads[0]:.1f}, max {leads[-1]:.1f})")
    print(f"  YES base rate     : {sum(e.outcome for e in events)/len(events):.1%}")

    # ── Stage 1: baseline ─────────────────────────────────────────────────────
    base_preds = lodo_cv_predictions(events, base, use_bias=False)
    base_brier = brier(base_preds)
    print("\n  ── Stage 1: baseline (no bias) ──")
    print(f"  CV Brier          : {base_brier:.4f}   (0=perfect, 0.25=coin flip, base-rate≈{_baserate_brier(events):.4f})")

    # ── Stage 2: + bias correction ────────────────────────────────────────────
    bias_preds = lodo_cv_predictions(events, base, use_bias=True, min_samples=args.min_samples)
    bias_brier = brier(bias_preds)
    full_bias = estimate_bias(events, base.source_weights or SOURCE_WEIGHTS, args.min_samples)
    print("\n  ── Stage 2: + per-(city,metric) bias, LODO-CV ──")
    print(f"  CV Brier          : {bias_brier:.4f}   (Δ {bias_brier-base_brier:+.4f} vs baseline)")
    print(f"  biases estimated  : {len(full_bias)} city/metric pairs (full-data values, for reference)")
    for (city, metric), b in sorted(full_bias.items(), key=lambda kv: -abs(kv[1]))[:12]:
        flag = "  ← grid runs COLD, shift up" if b > 1.0 else ("  ← grid runs WARM, shift down" if b < -1.0 else "")
        print(f"      {city:16s} {metric:4s}  {b:+5.1f}F{flag}")

    # ── Stage 3: coarse sweep (with LODO bias) ────────────────────────────────
    print("\n  ── Stage 3: coarse calibration sweep (+ LODO bias) ──")
    std_high_grid = [2.0, 3.0, 4.0, 5.0]
    std_low_grid  = [1.5, 2.0, 3.0, 4.0]
    ef_grid       = [0.5, 0.6, 0.7, 0.8]   # ensemble_fraction_weight; cdf = 1-ef
    results = []
    for sh in std_high_grid:
        for sl in std_low_grid:
            for ef in ef_grid:
                params = base.with_(std_floor_high=sh, std_floor_low=sl,
                                    ensemble_fraction_weight=ef, gaussian_cdf_weight=round(1 - ef, 3))
                preds = lodo_cv_predictions(events, params, use_bias=True, min_samples=args.min_samples)
                results.append({"std_high": sh, "std_low": sl, "ef_weight": ef,
                                "cdf_weight": round(1 - ef, 3), "cv_brier": round(brier(preds), 4)})
    results.sort(key=lambda r: r["cv_brier"])
    print(f"  swept {len(results)} configs (4×4×4). Top 8 by CV Brier:")
    print("    std_hi  std_lo  ef    cdf    CV-Brier")
    for r in results[:8]:
        print(f"    {r['std_high']:4.1f}    {r['std_low']:4.1f}   {r['ef_weight']:.2f}  {r['cdf_weight']:.2f}   {r['cv_brier']:.4f}")
    best = results[0]
    cur = next((r for r in results if r["std_high"] == 3.0 and r["std_low"] == 2.0 and r["ef_weight"] == 0.7), None)
    print(f"\n  current knobs (3.0/2.0/0.70) CV Brier: {cur['cv_brier'] if cur else 'n/a'}")
    print(f"  best config CV Brier                  : {best['cv_brier']}  "
          f"(std_hi={best['std_high']} std_lo={best['std_low']} ef={best['ef_weight']})")

    # ── Reliability of the best config ────────────────────────────────────────
    bp = base.with_(std_floor_high=best["std_high"], std_floor_low=best["std_low"],
                    ensemble_fraction_weight=best["ef_weight"], gaussian_cdf_weight=best["cdf_weight"])
    best_preds = lodo_cv_predictions(events, bp, use_bias=True, min_samples=args.min_samples)
    print("\n  ── Reliability (best config, + bias) ──")
    print(_fmt_rel(reliability_curve(best_preds)))

    out = {
        "window": {"start": str(days[0]), "end": str(days[-1]), "days": len(days)},
        "n_events": len(events),
        "lead_hours_median": leads[len(leads)//2],
        "baseline_cv_brier": round(base_brier, 4),
        "bias_cv_brier": round(bias_brier, 4),
        "biases": {f"{c}:{m}": round(b, 2) for (c, m), b in full_bias.items()},
        "sweep_top": results[:8],
        "best": best,
        "best_reliability": reliability_curve(best_preds),
    }
    Path(args.out).write_text(json.dumps(out, indent=2))
    print(f"\n  full results → {args.out}")
    print("\n  NOTE: 15 single-season days. These numbers describe summer calibration;")
    print("  treat improvements as provisional until the archive spans more weeks/seasons.")


def _baserate_brier(events):
    """Brier of always predicting the overall YES base rate — the naive reference."""
    rate = sum(e.outcome for e in events) / len(events)
    return sum((rate - e.outcome) ** 2 for e in events) / len(events)


if __name__ == "__main__":
    main()
