#!/usr/bin/env python3
"""
scripts/backfill_settlements.py — fill bt_settlements with ground-truth outcomes.

Thin CLI wrapper around weatherbot.core.backtest_settle.backfill_settlements.
Finds every captured ticker whose target_date has passed and that we don't yet
have a settlement for, then fetches the authoritative Kalshi `result` (yes/no)
and `expiration_value` (actual observed temperature) — the labels backtests need.

Run:
    source .venv/bin/activate
    python scripts/backfill_settlements.py [--days-back 30] [--limit 0]

Idempotent — safe to run repeatedly (the daily scheduler job calls the same fn).
"""
import argparse
import asyncio
import sys
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from weatherbot.core.backtest_settle import backfill_settlements


async def _run(days_back: int, limit: int) -> None:
    result = await backfill_settlements(days_back=days_back, limit=limit, verbose=True)
    print(f"\nDone. {result}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Backfill bt_settlements from Kalshi historical API")
    ap.add_argument("--days-back", type=int, default=30,
                    help="Look back this many days for unsettled tickers (default 30)")
    ap.add_argument("--limit", type=int, default=0,
                    help="Max tickers to process (0 = no limit)")
    args = ap.parse_args()
    asyncio.run(_run(args.days_back, args.limit))
