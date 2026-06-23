#!/usr/bin/env python3
"""
Engine fidelity / no-drift guardrail.

Replays captured bt_signal_snapshots through the shared backtest decide() core
with StrategyParams.live_default() and asserts it reproduces the model output the
LIVE bot recorded at scan time. This proves the backtest engine matches live
behavior, and — run regularly — fails loudly if the shared core drifts.

Honest caveats baked into the tolerances:
  * Archived ensemble members are rounded to 2 decimals (backtest_capture
    _source_summary) while the live signal used unrounded members, so
    model_probability is compared within a tolerance.
  * Live dynamic Brier weights depend on mutable ModelCityAccuracy state. While
    bt_settlements is empty that table is empty, so weights are static and
    deterministic; thresholds below are match-RATE based to tolerate later drift.
  * The same-scan weather snapshot is written a few seconds AFTER the signal row,
    so archive.weather_at forward-fills the earliest content — matching what was
    live at signal time.

Run:  .venv/bin/python -m pytest tests/test_engine_fidelity.py -s
  or: .venv/bin/python tests/test_engine_fidelity.py
"""
import sys
from datetime import timezone
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from sqlalchemy import text

from weatherbot.backtest.archive import load_archive
from weatherbot.backtest.decision import decide
from weatherbot.core.strategy import StrategyParams
from weatherbot.models.weather_db import SessionLocal

PROB_TOL = 0.03
EDGE_TOL = 0.03
SAMPLE = 4000


def run() -> bool:
    db = SessionLocal()
    try:
        # Clear any applied seasonal std-floor/blend overrides so this guardrail
        # tests decision-CODE fidelity, not the calibration config layer (same
        # reasoning as the grid_bias pin below). Without this, applying a seasonal
        # fit would replay new knobs against old-knob recordings.
        params = StrategyParams.live_default().with_(seasonal_overrides={})
        data = load_archive(db)  # whole archive

        # The earliest ~4000 signals this window covers were all captured BEFORE
        # per-city grid_bias was populated (every value was 0.0). The calibration
        # biases added to cities.json (2026-06-16) are cross-validated separately
        # in scripts/calibrate_archive.py; replaying them here would compare
        # new-model output against old-model recordings. Pin grid_bias to its
        # capture-time value (0) so this guardrail keeps testing decision-CODE
        # fidelity, not the bias config layer.
        from weatherbot.data.weather import CITY_CONFIG
        for _cfg in CITY_CONFIG.values():
            _cfg["grid_bias"] = {"high": 0.0, "low": 0.0}
            _cfg["grid_bias_seasonal"] = {}   # neutralize the seasonal bias layer too

        sig_rows = db.execute(text(
            "SELECT ticker, model_probability, market_probability, edge, bet_side, "
            "agreement, filter_reason, metric, captured_at FROM bt_signal_snapshots "
            "ORDER BY captured_at LIMIT :n"
        ), {"n": SAMPLE}).mappings().all()

        compared = prob_ok = agree_ok = edge_ok = side_ok = reason_ok = skipped = 0
        worst = []

        for s in sig_rows:
            if s["metric"] == "rain":
                skipped += 1
                continue
            as_of = s["captured_at"].replace(tzinfo=timezone.utc)
            wd = data.build_day(s["ticker"], as_of)
            if wd is None or not wd.sources or not any(sf.ok for sf in wd.sources.values()):
                skipped += 1
                continue
            dec = decide(wd, params)
            if dec is None:
                skipped += 1
                continue

            compared += 1
            dp = abs(dec.model_probability - s["model_probability"])
            if dp <= PROB_TOL:
                prob_ok += 1
            else:
                worst.append((round(dp, 4), s["ticker"], round(dec.model_probability, 4),
                              round(s["model_probability"], 4), dec.agreement, s["agreement"]))
            if (dec.agreement or "") == (s["agreement"] or ""):
                agree_ok += 1
            if abs(dec.edge - s["edge"]) <= EDGE_TOL:
                edge_ok += 1
            if (dec.direction or "") == (s["bet_side"] or ""):
                side_ok += 1
            if (dec.filter_reason or "") == (s["filter_reason"] or ""):
                reason_ok += 1

        print(f"\nFidelity over {compared} comparable signals (skipped {skipped} of {len(sig_rows)}):")
        if compared == 0:
            print("  No comparable rows — archive too sparse to test yet.")
            return True
        r = lambda x: 100.0 * x / compared
        print(f"  model_prob within {PROB_TOL}:  {prob_ok}/{compared}  ({r(prob_ok):.1f}%)")
        print(f"  agreement exact:          {agree_ok}/{compared}  ({r(agree_ok):.1f}%)")
        print(f"  edge within {EDGE_TOL}:        {edge_ok}/{compared}  ({r(edge_ok):.1f}%)")
        print(f"  bet_side exact:           {side_ok}/{compared}  ({r(side_ok):.1f}%)")
        print(f"  filter_reason exact:      {reason_ok}/{compared}  ({r(reason_ok):.1f}%)  "
              f"[informational: archive value is post-annotated by the live scan "
              f"loop with portfolio-stage reasons (below_edge/low_agreement) that "
              f"the decision-stage decide() doesn't emit — not asserted]")
        if worst:
            worst.sort(reverse=True)
            print("  largest model_prob diffs (decide vs live):")
            for d, tk, got, exp, ag, ale in worst[:8]:
                print(f"    {tk}: {got} vs {exp}  d={d}  agree {ag}/{ale}")

        ok = (r(prob_ok) >= 95 and r(agree_ok) >= 95 and r(side_ok) >= 95)
        return ok
    finally:
        db.close()


def test_engine_fidelity():
    assert run(), "backtest decide() drifted from captured live signals — see report"


if __name__ == "__main__":
    sys.exit(0 if run() else 1)
