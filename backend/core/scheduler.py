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

        # Log paper trades and send Discord alerts for new actionable signals
        from backend.core.paper_trading import log_paper_trade
        from backend.notifications.discord import send_paper_trade_alert

        for signal in actionable:
            ticker = signal.market.market_id

            # Always log paper trade (deduplication is inside log_paper_trade)
            trade = log_paper_trade(signal)

            if ticker not in _alerted_tickers:
                try:
                    if trade is not None:
                        # New paper trade — send paper trade alert
                        send_paper_trade_alert(signal, trade)
                    else:
                        # Already logged today — send regular signal alert
                        send_signal_alert(signal)
                    _alerted_tickers.add(ticker)
                except Exception as e:
                    logger.error(f"Failed to send Discord alert for {ticker}: {e}")

        if settings.DRY_RUN and actionable:
            logger.info(f"🔒 DRY RUN — logged {len(actionable)} paper trade(s), no real orders placed")

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


async def paper_settlement_job():
    """Hourly: settle paper trades whose resolution date has passed."""
    try:
        from backend.core.paper_trading import settle_paper_trades
        settled = await settle_paper_trades()
        if settled:
            wins   = sum(1 for t in settled if t.result == "win")
            losses = sum(1 for t in settled if t.result == "loss")
            pnl    = sum(t.pnl for t in settled if t.pnl is not None)
            logger.info(
                f"Paper trades settled: {len(settled)} ({wins}W/{losses}L)  P&L ${pnl:+.2f}"
            )
    except Exception as e:
        logger.error(f"Paper settlement error: {e}", exc_info=True)


async def paper_daily_summary_job():
    """Send paper trading daily summary to Discord at 11 PM ET (04:00 UTC next day)."""
    try:
        from backend.core.paper_trading import get_paper_stats, PaperSessionLocal, PaperTrade
        from backend.notifications.discord import send_paper_daily_summary
        from datetime import timezone

        db = PaperSessionLocal()
        try:
            stats = get_paper_stats(db)
            today_start = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
            resolved_today = [
                t for t in stats["resolved_trades"]
                if t.resolved_at and t.resolved_at >= today_start
            ]
            send_paper_daily_summary(stats, resolved_today)
        finally:
            db.close()
    except Exception as e:
        logger.error(f"Paper daily summary error: {e}", exc_info=True)


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

    # Paper trade settlement every hour
    scheduler.add_job(
        paper_settlement_job,
        IntervalTrigger(hours=1),
        id="paper_settlement",
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

    # Paper trading daily summary at 04:00 UTC (11 PM ET)
    scheduler.add_job(
        paper_daily_summary_job,
        CronTrigger(hour=4, minute=0),
        id="paper_daily_summary",
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
