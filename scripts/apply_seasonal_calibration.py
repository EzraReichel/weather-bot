#!/usr/bin/env python3
"""
Apply a season-aware calibration fit to the live config.

Reads the JSON produced by scripts/calibrate_archive.py and patches two files:
  • weatherbot/cities.json          — per-city ``grid_bias_seasonal[season]``
                                       from the seasonal bias buckets.
  • weatherbot/seasonal_overrides.json — per-season std-floor/blend knobs from
                                       ``best_by_season``, ONLY for seasons whose
                                       CV-Brier beats the baseline by --margin.

Both layers fall back to today's values for any season they don't cover, so a
partial (e.g. summer-only) fit is safe. Dry-run by default — pass --write to
actually patch the files. Keep this an explicit, reviewed step (mirrors how the
original grid-bias study was applied on a dated commit), not an auto side-effect
of calibration.

Usage:
    python scripts/apply_seasonal_calibration.py [--in calibration_archive.json]
                                                 [--margin 0.002] [--write]
"""
import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
CITIES_PATH = ROOT / "weatherbot" / "cities.json"
OVERRIDES_PATH = ROOT / "weatherbot" / "seasonal_overrides.json"

_KNOB_MAP = {
    "std_high": "std_floor_high",
    "std_low": "std_floor_low",
    "ef_weight": "ensemble_fraction_weight",
    "cdf_weight": "gaussian_cdf_weight",
}


def main():
    ap = argparse.ArgumentParser(description="Apply season-aware calibration to live config.")
    ap.add_argument("--in", dest="infile", default="calibration_archive.json")
    ap.add_argument("--margin", type=float, default=0.005,
                    help="min CV-Brier improvement a season's knobs must beat the "
                         "current-knobs+bias score by to be accepted (default 0.005 — "
                         "deliberately above ~20-day noise so marginal fits are rejected)")
    ap.add_argument("--write", action="store_true", help="actually patch the files (default: dry-run)")
    args = ap.parse_args()

    report = json.loads(Path(args.infile).read_text())
    # Gate per-season KNOBS against the current-knobs+bias score (bias_cv_brier),
    # not the no-bias baseline — otherwise the robust bias gain masks a marginal
    # knob change. The grid bias itself is applied unconditionally (it's the
    # well-established lever); only the std-floor/blend knobs are gated.
    knob_reference = report.get("bias_cv_brier", report.get("baseline_cv_brier"))
    biases_seasonal = report.get("biases_seasonal", {})
    best_by_season = report.get("best_by_season", {})

    # ── Seasonal grid bias → cities.json ──────────────────────────────────────
    cities = json.loads(CITIES_PATH.read_text())
    bias_changes = 0
    for key, val in biases_seasonal.items():
        season, city, metric = key.split(":")
        if city not in cities:
            print(f"  ! skip bias {key}: unknown city", file=sys.stderr)
            continue
        block = cities[city].setdefault("grid_bias_seasonal", {}).setdefault(season, {})
        if block.get(metric) != val:
            print(f"  bias  {season} {city:16s} {metric:4s}  {block.get(metric)} → {val:+.2f}")
            block[metric] = val
            bias_changes += 1

    # ── Seasonal knobs → seasonal_overrides.json (gated on CV-Brier) ──────────
    try:
        overrides = json.loads(OVERRIDES_PATH.read_text())
    except (FileNotFoundError, ValueError):
        overrides = {}
    knob_changes = 0
    for season, best in best_by_season.items():
        cv = best.get("cv_brier")
        if knob_reference is None or cv is None or cv > knob_reference - args.margin:
            print(f"  knobs {season}: CV-Brier {cv} does not beat current-knobs+bias "
                  f"{knob_reference} by {args.margin} — leaving fallback in place")
            continue
        ov = {dst: best[src] for src, dst in _KNOB_MAP.items() if src in best}
        if overrides.get(season) != ov:
            print(f"  knobs {season}: {overrides.get(season)} → {ov}  (CV-Brier {cv} < {baseline})")
            overrides[season] = ov
            knob_changes += 1

    print(f"\n  {bias_changes} grid-bias change(s), {knob_changes} knob change(s).")
    if not args.write:
        print("  DRY RUN — re-run with --write to patch cities.json / seasonal_overrides.json.")
        return

    if bias_changes:
        CITIES_PATH.write_text(json.dumps(cities, indent=2) + "\n")
        print(f"  wrote {CITIES_PATH}")
    if knob_changes:
        OVERRIDES_PATH.write_text(json.dumps(overrides, indent=2) + "\n")
        print(f"  wrote {OVERRIDES_PATH}")


if __name__ == "__main__":
    main()
