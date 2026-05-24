"""Background scheduler — weather scan, settlement, daily summary."""
import asyncio
import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Optional

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger
from sqlalchemy import distinct
from zoneinfo import ZoneInfo

from weatherbot.config import settings
from weatherbot.core.paper_trading import get_paper_stats
from weatherbot.core.trade_manager import execute_signal, settle_trades
from weatherbot.core.weather_signals import scan_for_weather_signals
from weatherbot.models.trade import SessionLocal as TradeSessionLocal, Trade
from weatherbot.models.weather_db import SessionLocal, Signal
from weatherbot.notifications.discord import (
    poll_discord_commands,
    send_daily_summary,
    send_live_trade_alert,
    send_paper_trade_alert,
    send_trade_settled_alert,
)

logger = logging.getLogger("weatherbot")

scheduler: Optional[AsyncIOScheduler] = None

# Track last alert time per ticker to enforce 6-hour dedup window
_alerted_tickers: dict = {}   # ticker -> datetime of last alert
_ALERT_DEDUP_HOURS = 6

# Latest scan report — used by daily summary job
_latest_scan_report = None


async def weather_scan_job():
    """Scan Kalshi weather markets, generate signals, fire Discord alerts."""
    global _latest_scan_report
    start = time.time()
    logger.info("── Weather scan started ──────────────────────────────────")

    try:
        scan = await scan_for_weather_signals()
        actionable = scan.actionable

        elapsed = time.time() - start
        logger.info(
            f"Scanned {len(scan.signals)} signals, {len(actionable)} above threshold "
            f"({elapsed:.1f}s)"
        )

        # Store latest scan report for daily summary
        _latest_scan_report = scan

        # ── Trading hours gate ────────────────────────────────────────────
        # Scan always runs (for data collection), but paper trading and
        # Discord alerts are suppressed outside 10am–6pm ET.
        _et_now = datetime.now(ZoneInfo("America/New_York"))
        _in_trading_hours = (
            settings.TRADING_HOURS_START <= _et_now.hour < settings.TRADING_HOURS_END
        )
        if not _in_trading_hours:
            logger.info(
                f"Outside trading hours ({settings.TRADING_HOURS_START}am–"
                f"{settings.TRADING_HOURS_END}pm ET, current={_et_now.strftime('%H:%M %Z')}) "
                f"— scan complete, skipping paper trade entries and Discord alerts"
            )
            return

        candidates = [s for s in scan.signals if s.passes_paper_threshold]

        for signal in candidates:
            ticker = signal.market.market_id
            trade = await execute_signal(signal)

            if trade is None:
                continue   # dedup or error — no alert

            last_alerted = _alerted_tickers.get(ticker)
            alert_cutoff = datetime.utcnow() - timedelta(hours=_ALERT_DEDUP_HOURS)
            if last_alerted is None or last_alerted <= alert_cutoff:
                try:
                    if trade.is_paper:
                        send_paper_trade_alert(signal, trade)
                    else:
                        send_live_trade_alert(signal, trade)
                    _alerted_tickers[ticker] = datetime.utcnow()
                except Exception as e:
                    logger.error(f"Failed to send Discord alert for {ticker}: {e}")

        if candidates:
            mode = "LIVE" if settings.LIVE_TRADING else "PAPER"
            logger.info(
                f"{'💸' if settings.LIVE_TRADING else '🔒'} {mode} — "
                f"{len(candidates)} trade(s) evaluated"
            )

    except Exception as e:
        logger.error(f"Weather scan error: {e}", exc_info=True)


async def discord_command_poll_job():
    """Poll Discord channel every 60s for 'report' commands."""
    try:
        poll_discord_commands()
    except Exception as e:
        logger.error(f"Discord command poll error: {e}", exc_info=True)


async def settlement_job():
    """Hourly: settle all pending trades (paper and live) whose resolution date has passed."""
    try:
        settled = await settle_trades()
        if settled:
            wins      = sum(1 for t in settled if t.result == "win")
            losses    = sum(1 for t in settled if t.result == "loss")
            cancelled = sum(1 for t in settled if t.result == "cancelled")
            pnl       = sum(t.pnl for t in settled if t.pnl is not None)
            logger.info(
                f"Trades settled: {len(settled)} ({wins}W/{losses}L/{cancelled} cancelled)  "
                f"P&L ${pnl:+.2f}"
            )
            stats = get_paper_stats()
            bankroll = settings.INITIAL_BANKROLL + stats["total_pnl"]
            for t in settled:
                try:
                    send_trade_settled_alert(t, bankroll=bankroll)
                except Exception as e:
                    logger.error(f"Failed to send settlement alert for {t.ticker}: {e}")
    except Exception as e:
        logger.error(f"Settlement error: {e}", exc_info=True)


async def daily_summary_job():
    """
    Send combined daily summary to Discord at 11:00 PM Eastern.
    Covers: unique signals found today, paper trades logged today, paper trades
    resolved today, running P&L, and Brier calibration score.
    """
    logger.info("Sending daily summary...")

    try:
        # "Today" in Eastern time so the window aligns with the 11 PM ET trigger
        now_et = datetime.now(ZoneInfo("America/New_York"))
        today_start_et = now_et.replace(hour=0, minute=0, second=0, microsecond=0)
        today_start_utc = today_start_et.astimezone(ZoneInfo("UTC")).replace(tzinfo=None)

        # ── Unique signal tickers seen today ──────────────────────────────────
        main_db = SessionLocal()
        try:
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
        paper_db = TradeSessionLocal()
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

            # Daily Brier: mean squared error only for trades settled today
            if resolved_today:
                daily_brier_scores = [
                    (t.model_prob - (1.0 if (t.actual_temp or 0) >= 1.0 else 0.0)) ** 2
                    for t in resolved_today
                ]
                daily_brier = sum(daily_brier_scores) / len(daily_brier_scores)
            else:
                daily_brier = None
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
            daily_brier=daily_brier,
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

    # Discord command poll every 60 seconds
    scheduler.add_job(
        discord_command_poll_job,
        IntervalTrigger(seconds=60),
        id="discord_poll",
        replace_existing=True,
        max_instances=1,
    )

    # Settlement every hour — handles both paper and live trades
    scheduler.add_job(
        settlement_job,
        IntervalTrigger(hours=1),
        id="settlement",
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
        f"Scheduler started — scan every {scan_secs}s, "
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
