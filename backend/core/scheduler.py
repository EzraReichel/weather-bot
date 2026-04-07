"""Background scheduler — weather scan, settlement, daily summary."""
import asyncio
import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Optional

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger
from sqlalchemy import func

from backend.config import settings
from backend.models.database import SessionLocal, Trade, BotState, Signal

logger = logging.getLogger("weatherbot")

scheduler: Optional[AsyncIOScheduler] = None

# Track which signal tickers we've already alerted on in this session
# to avoid spamming repeated alerts
_alerted_tickers: set = set()


async def weather_scan_job():
    """Scan Kalshi weather markets, generate signals, fire Discord alerts."""
    start = time.time()
    logger.info("── Weather scan started ──────────────────────────────────")

    try:
        from backend.core.weather_signals import scan_for_weather_signals
        from backend.notifications.discord import send_signal_alert

        signals = await scan_for_weather_signals()
        actionable = [s for s in signals if s.passes_threshold]

        elapsed = time.time() - start
        logger.info(
            f"Scanned {len(signals)} markets, found {len(actionable)} signals above threshold "
            f"({elapsed:.1f}s)"
        )

        # DRY RUN: log what WOULD be traded but don't place orders
        if settings.DRY_RUN and actionable:
            logger.info("🔒 DRY RUN — would place the following trades:")
            for s in actionable:
                logger.info(
                    f"  WOULD TRADE: {s.market.market_id}  {s.direction.upper()}  "
                    f"edge={s.edge:+.1%}  model={s.model_probability:.0%}  "
                    f"market={s.market_probability:.0%}  size=${s.suggested_size:.0f}"
                )

        # Send Discord alerts for new actionable signals
        for signal in actionable:
            ticker = signal.market.market_id
            if ticker not in _alerted_tickers:
                try:
                    send_signal_alert(signal)
                    _alerted_tickers.add(ticker)
                except Exception as e:
                    logger.error(f"Failed to send Discord alert for {ticker}: {e}")

        # Update bot state
        db = SessionLocal()
        try:
            state = db.query(BotState).first()
            if state:
                state.last_run = datetime.utcnow()
                db.commit()
        finally:
            db.close()

    except Exception as e:
        logger.error(f"Weather scan error: {e}", exc_info=True)


async def settlement_job():
    """Check and settle pending Kalshi weather trades."""
    logger.info("Checking trade settlements...")

    try:
        from backend.core.settlement import settle_pending_trades, update_bot_state_with_settlements

        db = SessionLocal()
        try:
            pending_count = db.query(Trade).filter(Trade.settled == False).count()
            if pending_count == 0:
                logger.debug("No pending trades to settle")
                return

            logger.info(f"Processing {pending_count} pending trades")
            settled = await settle_pending_trades(db)

            if settled:
                await update_bot_state_with_settlements(db, settled)
                wins = sum(1 for t in settled if t.result == "win")
                losses = sum(1 for t in settled if t.result == "loss")
                total_pnl = sum(t.pnl for t in settled if t.pnl is not None)
                logger.info(
                    f"Settled {len(settled)} trades: {wins}W/{losses}L  P&L: ${total_pnl:+.2f}"
                )
            else:
                logger.debug("No trades ready for settlement")
        finally:
            db.close()

    except Exception as e:
        logger.error(f"Settlement error: {e}", exc_info=True)


async def daily_summary_job():
    """Send end-of-day summary to Discord."""
    logger.info("Sending daily summary...")

    try:
        from backend.notifications.discord import send_daily_summary

        db = SessionLocal()
        try:
            today_start = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)

            total_signals = db.query(Signal).filter(Signal.timestamp >= today_start).count()
            actionable_signals = db.query(Signal).filter(
                Signal.timestamp >= today_start,
                Signal.edge >= settings.MIN_EDGE_THRESHOLD,
            ).count()
            trades_taken = db.query(Trade).filter(Trade.timestamp >= today_start).count()
            daily_pnl = db.query(func.coalesce(func.sum(Trade.pnl), 0.0)).filter(
                Trade.settled == True,
                Trade.settlement_time >= today_start,
            ).scalar() or 0.0

            state = db.query(BotState).first()
            bankroll = state.bankroll if state else settings.INITIAL_BANKROLL

            send_daily_summary(
                total_signals=total_signals,
                actionable_signals=actionable_signals,
                trades_taken=trades_taken,
                total_pnl=daily_pnl,
                bankroll=bankroll,
            )
        finally:
            db.close()

    except Exception as e:
        logger.error(f"Daily summary error: {e}", exc_info=True)


def start_scheduler():
    """Start the background scheduler."""
    global scheduler

    if scheduler is not None and scheduler.running:
        logger.warning("Scheduler already running")
        return

    scheduler = AsyncIOScheduler()

    scan_secs = settings.SCAN_INTERVAL_SECONDS

    scheduler.add_job(
        weather_scan_job,
        IntervalTrigger(seconds=scan_secs),
        id="weather_scan",
        replace_existing=True,
        max_instances=1,
    )

    # Settlement check every 30 minutes
    scheduler.add_job(
        settlement_job,
        IntervalTrigger(minutes=30),
        id="settlement",
        replace_existing=True,
        max_instances=1,
    )

    # Daily summary at 11:55 PM UTC
    scheduler.add_job(
        daily_summary_job,
        CronTrigger(hour=23, minute=55),
        id="daily_summary",
        replace_existing=True,
        max_instances=1,
    )

    scheduler.start()
    logger.info(
        f"Scheduler started — scan every {scan_secs}s, "
        f"settlement every 30m, daily summary at 23:55 UTC"
    )

    # Run first scan immediately
    asyncio.create_task(weather_scan_job())


def stop_scheduler():
    """Stop the background scheduler."""
    global scheduler
    if scheduler and scheduler.running:
        scheduler.shutdown(wait=False)
        scheduler = None
        logger.info("Scheduler stopped")


def is_scheduler_running() -> bool:
    return scheduler is not None and scheduler.running
