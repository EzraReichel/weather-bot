#!/usr/bin/env python3
"""
scripts/run_backtest.py — run a backtest from the command line.

Examples:
  # default strategy over a date range, all cities
  python scripts/run_backtest.py --start 2026-06-01 --end 2026-06-10

  # tweak a couple of knobs and a maker fill model
  python scripts/run_backtest.py --start 2026-06-01 --end 2026-06-10 \
      --set kelly_fraction=0.10 --set fill_mode=maker --set bankroll_basis=equity

  # a registered alternate strategy, restricted to two cities
  python scripts/run_backtest.py --start 2026-06-01 --end 2026-06-10 \
      --strategy contrarian --cities nyc,chicago --no-persist
"""
import argparse
import json
import sys
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from weatherbot.backtest.registry import list_strategies
from weatherbot.backtest.runner import run_backtest
from weatherbot.core.strategy import StrategyParams


def _coerce(v: str):
    low = v.lower()
    if low in ("true", "false"):
        return low == "true"
    try:
        return int(v)
    except ValueError:
        pass
    try:
        return float(v)
    except ValueError:
        return v


def main():
    ap = argparse.ArgumentParser(description="Run a weather-bot backtest.")
    ap.add_argument("--start", required=True, help="YYYY-MM-DD (inclusive)")
    ap.add_argument("--end", required=True, help="YYYY-MM-DD (inclusive)")
    ap.add_argument("--strategy", default="default", help=f"one of {list_strategies()}")
    ap.add_argument("--cities", default="", help="comma-separated city keys; empty = all")
    ap.add_argument("--set", action="append", default=[], metavar="KEY=VALUE",
                    help="override a StrategyParams field (repeatable)")
    ap.add_argument("--name", default=None)
    ap.add_argument("--no-persist", action="store_true", help="don't write bt_runs/bt_run_trades")
    args = ap.parse_args()

    overrides = {}
    for kv in args.set:
        if "=" not in kv:
            ap.error(f"--set expects KEY=VALUE, got {kv!r}")
        k, v = kv.split("=", 1)
        overrides[k.strip()] = _coerce(v.strip())

    params = StrategyParams.live_default().with_(**overrides) if overrides else StrategyParams.live_default()
    cities = [c.strip() for c in args.cities.split(",") if c.strip()] or None

    result = run_backtest(
        params=params, start=args.start, end=args.end, cities=cities,
        strategy=args.strategy, name=args.name, persist=not args.no_persist,
    )

    m = result["metrics"]
    print("=" * 64)
    print(f"  Backtest {result['run_id'][:8]}  strategy={result['strategy']}  "
          f"{args.start}→{args.end}  fill={params.fill_mode}  basis={params.bankroll_basis}")
    print("=" * 64)
    print(f"  signals evaluated : {m['n_signals']}")
    print(f"  filled            : {m['n_filled']}  (fill rate {m['fill_rate']}, bid-thin {m['bid_thin_fills']})")
    print(f"  resolved          : {m['n_resolved']}  (unsettled {m['n_unsettled']})")
    print(f"  wins / losses     : {m['wins']} / {m['losses']}  (hit rate {m['hit_rate']})")
    print(f"  total P&L         : ${m['total_pnl']:+.2f}")
    print(f"  bankroll          : ${m['initial_bankroll']:.2f} → ${m['final_bankroll']:.2f}  (ROI {m['roi']})")
    print(f"  Brier             : {m['brier']}")
    print(f"  max drawdown      : {m['max_drawdown']}")
    print(f"  turnover          : ${m['turnover']:.2f}")
    if m["by_city"]:
        print("  by city:")
        for c, b in sorted(m["by_city"].items()):
            print(f"    {c:16s} n={b['n']:3d}  {b['wins']}W/{b['losses']}L  P&L ${b['pnl']:+.2f}")
    if not args.no_persist:
        print(f"\n  persisted to bt_runs (run_id={result['run_id']})")


if __name__ == "__main__":
    main()
