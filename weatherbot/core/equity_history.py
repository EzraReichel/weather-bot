"""Equity snapshots — periodic records of live account value for the chart.

The bankroll graph plots real portfolio value (cash + market value of open
positions) over time. That can't be reconstructed from settled-trade P&L (which
values open positions at cost), so we snapshot the live account on a throttled
cadence from the scan loop and read the series back for the chart.
"""
import logging
from datetime import datetime, timedelta
from typing import Optional

from weatherbot.models.weather_db import SessionLocal, EquitySnapshot

logger = logging.getLogger("weatherbot")

# Default minimum spacing between snapshots. The scan loop runs every ~30s; one
# snapshot per 5 min is plenty of resolution for daily/weekly/monthly views
# without bloating the table.
DEFAULT_MIN_INTERVAL_S = 300


async def record_equity_snapshot(min_interval_s: int = DEFAULT_MIN_INTERVAL_S) -> Optional[EquitySnapshot]:
    """Record one {cash, positions, equity} snapshot, throttled.

    Skips if the most recent snapshot is newer than `min_interval_s`. Best-effort
    and self-contained: any failure is logged and swallowed by the caller so it
    can never interfere with trading. Returns the new row, or None if throttled.
    """
    db = SessionLocal()
    try:
        last = db.query(EquitySnapshot).order_by(EquitySnapshot.timestamp.desc()).first()
        if last and (datetime.utcnow() - last.timestamp).total_seconds() < min_interval_s:
            return None

        from weatherbot.data.kalshi_client import fetch_balance_breakdown
        b = await fetch_balance_breakdown()
        snap = EquitySnapshot(cash=b["cash"], positions=b["positions"], equity=b["equity"])
        db.add(snap)
        db.commit()
        db.refresh(snap)
        logger.debug(
            f"Equity snapshot: equity ${b['equity']:.2f} "
            f"(cash ${b['cash']:.2f} + positions ${b['positions']:.2f})"
        )
        return snap
    except Exception as e:
        logger.warning(f"Equity snapshot failed: {e}")
        db.rollback()
        return None
    finally:
        db.close()
