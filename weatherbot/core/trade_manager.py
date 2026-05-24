"""Trade lifecycle — entry routing, settlement, and stats for paper and live trades."""
import logging
from typing import List, Optional

from weatherbot.config import settings
from weatherbot.core.paper_trading import log_paper_trade, settle_paper_trades, get_paper_stats
from weatherbot.core.trading import log_live_trade, settle_live_trades, get_live_stats
from weatherbot.models.trade import Trade

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


def get_stats() -> dict:
    """Return stats for both paper and live trades."""
    return {
        "paper": get_paper_stats(),
        "live":  get_live_stats(),
    }
