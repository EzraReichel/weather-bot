"""Discord webhook notifications for weather arb signals."""
import logging
from datetime import datetime, timezone
from typing import List, Optional

import requests

from backend.config import settings

logger = logging.getLogger("weatherbot")

# Discord embed color constants
COLOR_GREEN = 0x2ECC71   # Actionable signal
COLOR_YELLOW = 0xF39C12  # Low-confidence signal
COLOR_BLUE = 0x3498DB    # Daily summary
COLOR_RED = 0xE74C3C     # Error/alert


def _post_embed(embed: dict) -> bool:
    """POST a single embed to Discord. Returns True on success."""
    url = settings.DISCORD_WEBHOOK_URL
    if not url:
        return False

    try:
        resp = requests.post(
            url,
            json={"embeds": [embed]},
            timeout=10,
        )
        if resp.status_code in (200, 204):
            return True
        logger.warning(f"Discord webhook returned {resp.status_code}: {resp.text[:200]}")
        return False
    except Exception as e:
        logger.error(f"Discord webhook failed: {e}")
        return False


def send_signal_alert(signal) -> bool:
    """
    Send a Discord alert for an actionable weather signal.

    Args:
        signal: WeatherTradingSignal instance
    """
    if not settings.DISCORD_WEBHOOK_URL:
        return False

    market = signal.market
    color = COLOR_YELLOW if signal.low_confidence_flag else COLOR_GREEN

    # Kelly size rounded to nearest dollar
    kelly_amount = f"${signal.suggested_size:.0f}"

    # Side label
    side = signal.direction.upper()

    # Confidence label
    conf_pct = f"{signal.confidence:.0%}"
    low_conf_note = "  ⚠️ Low Confidence (CDF vs fraction disagree)" if signal.low_confidence_flag else ""

    fields = [
        {"name": "Ticker", "value": f"`{market.market_id}`", "inline": True},
        {"name": "Side", "value": f"**{side}**", "inline": True},
        {"name": "Edge", "value": f"**{signal.edge:+.1%}**", "inline": True},
        {"name": "Model Prob", "value": f"{signal.model_probability:.1%}", "inline": True},
        {"name": "Market Price", "value": f"{signal.market_probability:.1%}", "inline": True},
        {"name": "Kelly Size", "value": kelly_amount, "inline": True},
        {"name": "Confidence", "value": conf_pct + low_conf_note, "inline": False},
        {
            "name": "Forecast",
            "value": (
                f"Mean: {signal.ensemble_mean:.1f}°F  |  "
                f"Std: {signal.ensemble_std:.1f}°F  |  "
                f"Members: {signal.ensemble_members}"
            ),
            "inline": False,
        },
    ]

    embed = {
        "title": f"🌡️ {market.title}",
        "description": (
            f"{market.city_name} — {market.metric.upper()} temp "
            f"**{market.direction}** {market.threshold_f:.0f}°F on {market.target_date}"
        ),
        "color": color,
        "fields": fields,
        "footer": {"text": "Kalshi Weather Arb Bot"},
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    success = _post_embed(embed)
    if success:
        logger.info(f"Discord alert sent: {market.market_id} {side} edge={signal.edge:+.1%}")
    return success


def send_daily_summary(
    total_signals: int,
    actionable_signals: int,
    trades_taken: int,
    total_pnl: float,
    bankroll: float,
) -> bool:
    """Send end-of-day summary to Discord."""
    if not settings.DISCORD_WEBHOOK_URL:
        return False

    pnl_sign = "+" if total_pnl >= 0 else ""
    color = COLOR_GREEN if total_pnl >= 0 else COLOR_RED

    embed = {
        "title": "📊 Daily Summary",
        "color": color,
        "fields": [
            {"name": "Signals Found", "value": str(total_signals), "inline": True},
            {"name": "Actionable", "value": str(actionable_signals), "inline": True},
            {"name": "Trades Taken", "value": str(trades_taken), "inline": True},
            {"name": "P&L", "value": f"**{pnl_sign}${total_pnl:.2f}**", "inline": True},
            {"name": "Bankroll", "value": f"${bankroll:,.2f}", "inline": True},
        ],
        "footer": {"text": "Kalshi Weather Arb Bot"},
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    success = _post_embed(embed)
    if success:
        logger.info("Discord daily summary sent")
    return success


def send_paper_trade_alert(signal, trade) -> bool:
    """
    Send a Discord alert for a logged paper trade.
    Prefixed with '📝 PAPER TRADE' so it's clearly not a real order.
    """
    if not settings.DISCORD_WEBHOOK_URL:
        return False

    market = signal.market
    color = COLOR_YELLOW if signal.low_confidence_flag else COLOR_GREEN
    side = signal.direction.upper()
    conf_pct = f"{signal.confidence:.0%}"
    low_conf_note = "  ⚠️ Low Confidence" if signal.low_confidence_flag else ""

    fields = [
        {"name": "Ticker",       "value": f"`{market.market_id}`",              "inline": True},
        {"name": "Side",         "value": f"**{side}**",                         "inline": True},
        {"name": "Edge",         "value": f"**{signal.edge:+.1%}**",             "inline": True},
        {"name": "Model Prob",   "value": f"{signal.model_probability:.1%}",     "inline": True},
        {"name": "Market Price", "value": f"{signal.market_probability:.1%}",    "inline": True},
        {"name": "Kelly Size",   "value": f"${signal.suggested_size:.0f}",       "inline": True},
        {"name": "Contracts",    "value": str(trade.contracts),                  "inline": True},
        {"name": "Entry Price",  "value": f"{trade.entry_price:.2%}",            "inline": True},
        {"name": "Confidence",   "value": conf_pct + low_conf_note,              "inline": True},
        {
            "name": "Forecast",
            "value": (
                f"Mean: {signal.ensemble_mean:.1f}°F  |  "
                f"Std: {signal.ensemble_std:.1f}°F  |  "
                f"Members: {signal.ensemble_members}"
            ),
            "inline": False,
        },
    ]

    embed = {
        "title": f"📝 PAPER TRADE — {market.title}",
        "description": (
            f"{market.city_name} — {market.metric.upper()} temp "
            f"**{market.direction}** {market.threshold_f:.0f}°F on {market.target_date}\n"
            f"*This is a paper trade. No real order was placed.*"
        ),
        "color": color,
        "fields": fields,
        "footer": {"text": "Kalshi Weather Arb Bot · Paper Trading"},
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    success = _post_embed(embed)
    if success:
        logger.info(f"Discord paper trade alert sent: {market.market_id} {side}")
    return success


def send_paper_daily_summary(stats: dict, resolved_today: list) -> bool:
    """Send the paper trading daily summary at 11 PM ET."""
    if not settings.DISCORD_WEBHOOK_URL:
        return False

    pnl = stats["total_pnl"]
    pnl_sign = "+" if pnl >= 0 else ""
    color = COLOR_GREEN if pnl >= 0 else COLOR_RED
    win_rate = (stats["wins"] / stats["resolved"] * 100) if stats["resolved"] > 0 else 0.0
    brier_str = f"{stats['brier']:.3f}" if stats["brier"] is not None else "n/a"

    # Resolved-today lines
    resolved_lines = []
    for t in resolved_today:
        icon = "✅" if t.result == "win" else "❌"
        resolved_lines.append(
            f"{icon} `{t.ticker}` — actual {t.actual_temp:.1f}°F vs {t.threshold_f:.0f}°F  P&L ${t.pnl:+.2f}"
        )
    resolved_text = "\n".join(resolved_lines) if resolved_lines else "None today"

    fields = [
        {"name": "Total Paper Trades", "value": str(stats["total"]),        "inline": True},
        {"name": "Resolved",           "value": str(stats["resolved"]),      "inline": True},
        {"name": "Pending",            "value": str(stats["unresolved"]),    "inline": True},
        {"name": "W/L Record",         "value": f"{stats['wins']}W / {stats['losses']}L  ({win_rate:.0f}%)", "inline": True},
        {"name": "Running P&L",        "value": f"**{pnl_sign}${pnl:.2f}**","inline": True},
        {"name": "Brier Score",        "value": brier_str,                   "inline": True},
        {"name": "Avg Edge at Entry",  "value": f"{stats['avg_edge']:+.1%}", "inline": True},
        {"name": "Resolved Today",     "value": resolved_text,               "inline": False},
    ]

    embed = {
        "title": "📋 Paper Trading Daily Summary",
        "color": color,
        "fields": fields,
        "footer": {"text": "Kalshi Weather Arb Bot · Paper Trading"},
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    success = _post_embed(embed)
    if success:
        logger.info("Discord paper daily summary sent")
    return success


def send_startup_message(simulation_mode: bool, bankroll: float) -> bool:
    """Send a startup notification."""
    if not settings.DISCORD_WEBHOOK_URL:
        return False

    mode = "SIMULATION" if simulation_mode else "LIVE"
    embed = {
        "title": "🚀 Bot Started",
        "description": f"Mode: **{mode}** | Bankroll: **${bankroll:,.2f}**",
        "color": COLOR_BLUE,
        "fields": [
            {"name": "Scan Interval", "value": f"{settings.SCAN_INTERVAL_SECONDS}s", "inline": True},
            {"name": "Min Edge", "value": f"{settings.MIN_EDGE_THRESHOLD:.0%}", "inline": True},
            {"name": "Kelly Fraction", "value": f"{settings.KELLY_FRACTION:.0%}", "inline": True},
            {"name": "Fee Rate", "value": f"{settings.KALSHI_FEE_RATE:.0%}", "inline": True},
        ],
        "footer": {"text": "Kalshi Weather Arb Bot"},
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    return _post_embed(embed)
