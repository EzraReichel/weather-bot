"""Background scheduler — weather scan, settlement, daily summary."""
import asyncio
import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Optional

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger
from zoneinfo import ZoneInfo

from weatherbot.config import settings
from weatherbot.core.paper_trading import get_paper_stats
from weatherbot.core.trade_manager import execute_signal, settle_trades
from weatherbot.core.trading import get_live_stats
from weatherbot.core.weather_signals import scan_for_weather_signals
from weatherbot.data.kalshi_client import fetch_live_balance
from weatherbot.models.trade import SessionLocal as TradeSessionLocal
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


async def weather_scan_job():
    """Scan Kalshi weather markets, generate signals, fire Discord alerts."""
    start = time.time()
    logger.info("── Weather scan started ──────────────────────────────────")

    try:
        # Prune stale dedup entries so the dict doesn't grow unbounded
        cutoff = datetime.utcnow() - timedelta(hours=_ALERT_DEDUP_HOURS)
        stale = [k for k, v in _alerted_tickers.items() if v <= cutoff]
        for k in stale:
            del _alerted_tickers[k]

        scan = await scan_for_weather_signals()
        actionable = scan.actionable

        elapsed = time.time() - start
        logger.info(
            f"Scanned {len(scan.signals)} signals, {len(actionable)} above threshold "
            f"({elapsed:.1f}s)"
        )

        candidates = [s for s in scan.signals if (
            s.passes_threshold if settings.LIVE_TRADING else s.passes_paper_threshold
        )]

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

        # ── Backtest data capture (best-effort, never blocks trading) ─────────
        # Runs after trade execution so capture latency can't delay an order.
        try:
            from weatherbot.core.backtest_capture import capture_scan
            await capture_scan(scan, trigger="interval")
        except Exception as e:
            logger.debug(f"Backtest capture skipped: {e}")

    except Exception as e:
        logger.error(f"Weather scan error: {e}", exc_info=True)


async def discord_command_poll_job():
    """Poll Discord channel every 60s for 'report' commands."""
    try:
        bankroll = await fetch_live_balance()
        poll_discord_commands(bankroll=bankroll)
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
            bankroll = await fetch_live_balance()
            for t in settled:
                try:
                    send_trade_settled_alert(t, bankroll=bankroll)
                except Exception as e:
                    logger.error(f"Failed to send settlement alert for {t.ticker}: {e}")
    except Exception as e:
        logger.error(f"Settlement error: {e}", exc_info=True)


async def backtest_settlement_backfill_job():
    """Daily: fetch ground-truth outcomes for captured markets that have settled."""
    if not settings.BACKTEST_CAPTURE:
        return
    try:
        from weatherbot.core.backtest_settle import backfill_settlements
        await backfill_settlements(days_back=7, limit=0)
    except Exception as e:
        logger.error(f"Backtest settlement backfill error: {e}", exc_info=True)


async def daily_summary_job():
    """
    Send combined daily summary to Discord at 11:00 PM Eastern.
    Covers: trades logged today, trades resolved today, running P&L, bankroll,
    and Brier calibration score for the active trading mode.
    """
    logger.info("Sending daily summary...")

    try:
        # "Today" in Eastern time so the window aligns with the 11 PM ET trigger
        now_et = datetime.now(ZoneInfo("America/New_York"))
        today_start_et = now_et.replace(hour=0, minute=0, second=0, microsecond=0)
        today_start_utc = today_start_et.astimezone(ZoneInfo("UTC")).replace(tzinfo=None)

        # ── Trades logged / resolved today (active mode only) ─────────────────
        # Report on whichever mode the bot is actually running so P&L, counts,
        # and bankroll stay internally consistent. Mixing historical paper P&L
        # into a live running total is what made the summary look "weird".
        live = settings.LIVE_TRADING
        trade_db = TradeSessionLocal()
        try:
            stats = get_live_stats(trade_db) if live else get_paper_stats(trade_db)

            logged_today = [
                t for t in stats["all_trades"]
                if t.created_at and t.created_at >= today_start_utc
            ]
            resolved_today = [
                t for t in stats["resolved_trades"]
                if t.resolved_at and t.resolved_at >= today_start_utc
            ]
            daily_pnl = sum(t.pnl for t in resolved_today if t.pnl is not None)

            # Daily Brier: mean squared error only for trades settled today
            daily_brier_scores = [
                (t.model_prob - (1.0 if (t.actual_temp or 0) >= 1.0 else 0.0)) ** 2
                for t in resolved_today
                if t.model_prob is not None
            ]
            daily_brier = (
                sum(daily_brier_scores) / len(daily_brier_scores)
                if daily_brier_scores else None
            )
        finally:
            trade_db.close()

        bankroll = await fetch_live_balance()
        send_daily_summary(
            logged_today=logged_today,
            resolved_today=resolved_today,
            daily_pnl=daily_pnl,
            stats=stats,
            daily_brier=daily_brier,
            bankroll=bankroll,
            live_trading=live,
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

    # Backtest settlement backfill — once daily, after midnight ET so the
    # previous day's markets have settled on Kalshi.
    if settings.BACKTEST_CAPTURE:
        scheduler.add_job(
            backtest_settlement_backfill_job,
            CronTrigger(hour=2, minute=0, timezone="America/New_York"),
            id="bt_settlement_backfill",
            replace_existing=True,
            max_instances=1,
        )

    scheduler.start()
    logger.info(
        f"Scheduler started — scan every {scan_secs}s, "
        f"paper settlement every 1h, model-run scans at 03:30/09:30/15:30/21:30 ET, "
        f"daily summary at 23:00 ET"
    )

    # Run first scan and settlement immediately on startup
    asyncio.create_task(weather_scan_job())
    asyncio.create_task(settlement_job())


def stop_scheduler():
    """Stop the background scheduler."""
    global scheduler
    if scheduler and scheduler.running:
        scheduler.shutdown(wait=False)
        scheduler = None
        logger.info("Scheduler stopped")


def is_scheduler_running() -> bool:
    return scheduler is not None and scheduler.running
