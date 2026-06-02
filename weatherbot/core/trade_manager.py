"""Trade lifecycle — entry routing, settlement, and stats for paper and live trades."""
import logging
from datetime import datetime
from typing import Callable, List, Optional

from weatherbot.config import settings
from weatherbot.core.paper_trading import log_paper_trade, settle_paper_trades, get_paper_stats
from weatherbot.core.trading import log_live_trade, settle_live_trades, get_live_stats
from weatherbot.models.trade import Trade
from weatherbot.models.weather_db import SessionLocal

logger = logging.getLogger("weatherbot")


async def execute_signal(signal) -> Optional[Trade]:
    """
    Route a signal to paper or live entry.
    Returns the logged Trade, or None if skipped (dedup, error, etc.).
    """
    if not settings.LIVE_TRADING:
        return log_paper_trade(signal)

    assert "demo" not in settings.KALSHI_API_BASE_URL, (
        "LIVE_TRADING=true but KALSHI_API_BASE_URL points at the demo API — "
        "set KALSHI_API_BASE_URL=https://api.kalshi.com/trade-api/v2 in .env"
    )
    assert settings.KALSHI_API_KEY_ID, "LIVE_TRADING=true but KALSHI_API_KEY_ID is not set"

    return await log_live_trade(signal)


async def settle_trades() -> List[Trade]:
    """
    Settle all pending trades — both paper and live — whose resolution date has passed.
    Runs both regardless of LIVE_TRADING flag so existing trades always get resolved.
    """
    settled = []
    settled.extend(await settle_paper_trades())
    settled.extend(await settle_live_trades())
    return settled


def notify_pending_settlements(send_alert: Callable[[Trade], bool]) -> int:
    """
    Send settlement alerts for every resolved trade not yet notified, then stamp
    `notified_at` so it can't be alerted again.

    This is decoupled from settle_trades() on purpose: settlement persists
    `resolved` first, and notification is driven off the DB (resolved &
    notified_at IS NULL) rather than off whatever a settle call returned. A
    rollback never reaches here (nothing is persisted), and a restart can't
    re-alert an already-stamped trade — so alerts are sent exactly once.

    `send_alert(trade) -> bool` does the actual send; we only stamp on success,
    so a transient Discord failure retries next cycle instead of being lost.
    Returns the number of alerts successfully sent.
    """
    db = SessionLocal()
    sent = 0
    try:
        pending = db.query(Trade).filter(
            Trade.resolved == True,
            Trade.notified_at == None,  # noqa: E711 — SQL IS NULL
        ).order_by(Trade.resolved_at).all()

        for trade in pending:
            try:
                if send_alert(trade):
                    trade.notified_at = datetime.utcnow()
                    db.commit()
                    sent += 1
                else:
                    db.rollback()
            except Exception as e:
                logger.error(f"Failed to send settlement alert for {trade.ticker}: {e}")
                db.rollback()
    except Exception as e:
        logger.error(f"Settlement notification pass error: {e}", exc_info=True)
        db.rollback()
    finally:
        db.close()

    return sent


def get_stats() -> dict:
    """Return stats for both paper and live trades."""
    return {
        "paper": get_paper_stats(),
        "live":  get_live_stats(),
    }
