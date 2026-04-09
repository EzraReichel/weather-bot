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

# Track last alert time per ticker to enforce 6-hour dedup window
_alerted_tickers: dict = {}   # ticker -> datetime of last alert
_ALERT_DEDUP_HOURS = 6

# Latest scan report — used by daily summary job
_latest_scan_report = None


async def weather_scan_job():
    """Scan Kalshi weather markets, generate signals, fire Discord alerts."""
    start = time.time()
    logger.info("── Weather scan started ──────────────────────────────────")

    try:
        from backend.core.weather_signals import scan_for_weather_signals
        from backend.notifications.discord import send_signal_alert

        scan = await scan_for_weather_signals()
        actionable = scan.actionable

        elapsed = time.time() - start
        logger.info(
            f"Scanned {len(scan.signals)} signals, {len(actionable)} above threshold "
            f"({elapsed:.1f}s)"
        )

        # Store latest scan report for daily summary
        _latest_scan_report = scan

        # Log paper trades and send Discord alerts for new actionable signals
        from backend.core.paper_trading import log_paper_trade
        from backend.notifications.discord import send_paper_trade_alert

        for signal in actionable:
            ticker = signal.market.market_id
            trade = log_paper_trade(signal)

            last_alerted = _alerted_tickers.get(ticker)
            alert_cutoff = datetime.utcnow() - timedelta(hours=_ALERT_DEDUP_HOURS)
            already_alerted = last_alerted is not None and last_alerted > alert_cutoff

            if not already_alerted:
                try:
                    if trade is not None:
                        send_paper_trade_alert(signal, trade)
                    else:
                        send_signal_alert(signal)
                    _alerted_tickers[ticker] = datetime.utcnow()
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


async def daily_summary_job():
    """
    Send combined daily summary to Discord at 11:00 PM Eastern.
    Covers: unique signals found today, paper trades logged today, paper trades
    resolved today, running P&L, and Brier calibration score.
    """
    from zoneinfo import ZoneInfo
    logger.info("Sending daily summary...")

    try:
        from backend.notifications.discord import send_daily_summary
        from backend.core.paper_trading import get_paper_stats
        from backend.models.paper_trade import PaperSessionLocal, PaperTrade

        # "Today" in Eastern time so the window aligns with the 11 PM ET trigger
        et = ZoneInfo("America/New_York")
        now_et = datetime.now(et)
        today_start_et = now_et.replace(hour=0, minute=0, second=0, microsecond=0)
        today_start_utc = today_start_et.astimezone(ZoneInfo("UTC")).replace(tzinfo=None)

        # ── Unique signal tickers seen today ──────────────────────────────────
        main_db = SessionLocal()
        try:
            from sqlalchemy import distinct
            unique_tickers = (
                main_db.query(distinct(Signal.market_ticker))
                .filter(Signal.timestamp >= today_start_utc)
                .count()
            )
            actionable_tickers = (
                main_db.query(distinct(Signal.market_ticker))
                .filter(
                    Signal.timestamp >= today_start_utc,
                    Signal.edge >= settings.MIN_EDGE_THRESHOLD,
                )
                .count()
            )
        finally:
            main_db.close()

        # ── Paper trades logged today ─────────────────────────────────────────
        paper_db = PaperSessionLocal()
        try:
            paper_stats = get_paper_stats(paper_db)

            logged_today = [
                t for t in paper_stats["all_trades"]
                if t.created_at >= today_start_utc
            ]
            resolved_today = [
                t for t in paper_stats["resolved_trades"]
                if t.resolved_at and t.resolved_at >= today_start_utc
            ]
            daily_paper_pnl = sum(t.pnl for t in resolved_today if t.pnl is not None)
        finally:
            paper_db.close()

        send_daily_summary(
            unique_signals=unique_tickers,
            actionable_signals=actionable_tickers,
            paper_logged_today=logged_today,
            paper_resolved_today=resolved_today,
            daily_paper_pnl=daily_paper_pnl,
            paper_stats=paper_stats,
            scan_report=_latest_scan_report,
        )

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

    # Model-run trigger scans — fire 15 minutes after GFS/ECMWF publish
    # GFS runs: ~00Z, 06Z, 12Z, 18Z → products available ~03:30, 09:30, 15:30, 21:30 UTC
    # In ET: 23:30, 05:30, 11:30, 17:30 (standard) / 00:30, 06:30, 12:30, 18:30 (DST)
    # We use ET via timezone param so DST is handled automatically
    for hour, label in [(3, "00Z"), (9, "06Z"), (15, "12Z"), (21, "18Z")]:
        scheduler.add_job(
            weather_scan_job,
            CronTrigger(hour=hour, minute=30, timezone="America/New_York"),
            id=f"model_run_scan_{label}",
            replace_existing=True,
            max_instances=1,
        )

    # Combined daily summary at 11:00 PM Eastern (America/New_York handles DST)
    scheduler.add_job(
        daily_summary_job,
        CronTrigger(hour=23, minute=0, timezone="America/New_York"),
        id="daily_summary",
        replace_existing=True,
        max_instances=1,
    )

    scheduler.start()
    logger.info(
        f"Scheduler started — scan every {scan_secs}s, settlement every 30m, "
        f"paper settlement every 1h, model-run scans at 03:30/09:30/15:30/21:30 ET, "
        f"daily summary at 23:00 ET"
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
