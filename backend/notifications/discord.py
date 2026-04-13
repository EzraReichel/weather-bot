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

    # Per-source probability breakdown
    sp = getattr(signal, "source_probs", {})
    agreement = getattr(signal, "agreement", "MEDIUM")
    outlier = getattr(signal, "outlier_dampened", None)
    agreement_icon = {"HIGH": "🟢", "MEDIUM": "🟡", "LOW": "🔴"}.get(agreement, "🟡")

    source_parts = []
    for name in ["gfs", "ecmwf", "gem", "nws"]:
        if name in sp:
            tag = " ⚡dampened" if name == outlier else ""
            source_parts.append(f"{name.upper()}: {sp[name]:.0%}{tag}")
    source_breakdown = "  |  ".join(source_parts) if source_parts else "GFS only"
    if outlier:
        source_breakdown += f"\n*{outlier.upper()} weight halved (outlier >40% from others)*"

    fields = [
        {"name": "Ticker",        "value": f"`{market.market_id}`",           "inline": True},
        {"name": "Side",          "value": f"**{side}**",                      "inline": True},
        {"name": "Edge",          "value": f"**{signal.edge:+.1%}**",          "inline": True},
        {"name": "Combined Prob", "value": f"{signal.model_probability:.1%}",  "inline": True},
        {"name": "Market Price",  "value": f"{signal.market_probability:.1%}", "inline": True},
        {"name": "Kelly Size",    "value": kelly_amount,                       "inline": True},
        {
            "name": "Model Breakdown",
            "value": source_breakdown,
            "inline": False,
        },
        {
            "name": "Agreement",
            "value": f"{agreement_icon} **{agreement}**" + low_conf_note,
            "inline": True,
        },
        {"name": "Confidence", "value": conf_pct, "inline": True},
        {
            "name": "Forecast (GFS ref)",
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


def _build_filter_report_text(scan_report) -> str:
    """Build a compact filter breakdown string from a ScanReport."""
    if scan_report is None:
        return "No scan data available."

    fr = scan_report.fetch_report
    lines = []

    # Liquidity filtered
    liq = [f for f in fr.filtered if f.reason in ("low_ask", "low_volume")] if fr else []
    if liq:
        liq_lines = []
        for f in liq[:8]:   # cap at 8 to stay under Discord 1024-char limit
            if f.reason == "low_ask":
                liq_lines.append(f"`{f.ticker}` ask={f.ask_size:.0f}")
            else:
                liq_lines.append(f"`{f.ticker}` vol={f.volume_24h:.0f}")
        if len(liq) > 8:
            liq_lines.append(f"…+{len(liq)-8} more")
        lines.append(f"**Liquidity ({len(liq)}):** " + "  ".join(liq_lines))
    else:
        lines.append("**Liquidity:** none filtered")

    # Low agreement
    la = scan_report.low_agreement_filtered or []
    if la:
        la_lines = []
        for s in la[:6]:
            sp = s.source_probs
            parts = [f"{n.upper()}={sp[n]:.0%}" for n in ["gfs","ecmwf","gem","nws"] if n in sp]
            la_lines.append(f"`{s.market.market_id}` edge={s.edge:+.0%} [{' '.join(parts)}]")
        if len(la) > 6:
            la_lines.append(f"…+{len(la)-6} more")
        lines.append(f"**Low agreement ({len(la)}):**\n" + "\n".join(la_lines))
    else:
        lines.append("**Low agreement:** none")

    # Below edge threshold
    be = scan_report.below_edge or []
    if be:
        be_lines = [f"`{s.market.market_id}` {s.edge:+.1%}" for s in be[:8]]
        if len(be) > 8:
            be_lines.append(f"…+{len(be)-8} more")
        lines.append(f"**Below edge ({len(be)}):** " + "  ".join(be_lines))
    else:
        lines.append("**Below edge:** none")

    return "\n".join(lines)


def send_daily_summary(
    unique_signals: int,
    actionable_signals: int,
    paper_logged_today: list,
    paper_resolved_today: list,
    daily_paper_pnl: float,
    paper_stats: dict,
    scan_report=None,
) -> bool:
    """Send combined end-of-day summary to Discord at 11 PM ET."""
    if not settings.DISCORD_WEBHOOK_URL:
        return False

    pnl_sign = "+" if daily_paper_pnl >= 0 else ""
    running_pnl = paper_stats.get("total_pnl", 0.0)
    running_sign = "+" if running_pnl >= 0 else ""
    color = COLOR_GREEN if running_pnl >= 0 else COLOR_RED

    wins  = paper_stats.get("wins", 0)
    losses = paper_stats.get("losses", 0)
    brier = paper_stats.get("brier")
    brier_str = f"{brier:.3f}" if brier is not None else "n/a"

    # Paper trades logged today — brief list
    if paper_logged_today:
        logged_lines = [
            f"`{t.ticker}` — {t.side.upper()} edge={t.edge:+.1%} @ {t.entry_price:.2%}"
            for t in paper_logged_today
        ]
        logged_text = "\n".join(logged_lines)
    else:
        logged_text = "None today"

    # Paper trades resolved today
    if paper_resolved_today:
        resolved_lines = []
        for t in paper_resolved_today:
            icon = "✅" if t.result == "win" else "❌"
            kalshi_result = "YES" if (t.actual_temp or 0) >= 1.0 else "NO"
            resolved_lines.append(
                f"{icon} `{t.ticker}` Kalshi={kalshi_result}  side={t.side.upper()}  ${t.pnl:+.2f}"
            )
        resolved_text = "\n".join(resolved_lines)
    else:
        resolved_text = "None settled today"

    # Filter report — markets scanned vs filtered
    fr = scan_report.fetch_report if scan_report else None
    total_raw      = fr.total_raw if fr else 0
    series_scanned = fr.series_scanned if fr else 0
    liq_filtered   = len([f for f in fr.filtered if f.reason in ("low_ask","low_volume")]) if fr else 0
    brackets       = len([f for f in fr.filtered if f.reason == "bracket"]) if fr else 0
    passed_liq     = len(fr.markets) if fr else 0
    filter_detail  = _build_filter_report_text(scan_report)

    fields = [
        {"name": "Series Scanned",       "value": str(series_scanned),         "inline": True},
        {"name": "Raw Markets",          "value": str(total_raw),               "inline": True},
        {"name": "Passed Liquidity",     "value": str(passed_liq),             "inline": True},
        {"name": "Unique Signals",       "value": str(unique_signals),         "inline": True},
        {"name": "Actionable",           "value": f"**{actionable_signals}**", "inline": True},
        {"name": "Paper Trades Today",   "value": str(len(paper_logged_today)),"inline": True},
        {"name": "Today's P&L",          "value": f"**{pnl_sign}${daily_paper_pnl:.2f}**", "inline": True},
        {"name": "Running P&L",          "value": f"**{running_sign}${running_pnl:.2f}**", "inline": True},
        {"name": "All-time W/L",         "value": f"{wins}W / {losses}L",     "inline": True},
        {"name": "Brier Score",          "value": brier_str,                   "inline": True},
        {"name": "Filter Breakdown",     "value": filter_detail,               "inline": False},
        {"name": "New Paper Trades",     "value": logged_text,                 "inline": False},
        {"name": "Resolved Today",       "value": resolved_text,               "inline": False},
    ]

    embed = {
        "title": "📊 Daily Summary",
        "color": color,
        "fields": fields,
        "footer": {"text": "Kalshi Weather Arb Bot · 11 PM ET"},
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

    sp = getattr(signal, "source_probs", {})
    agreement = getattr(signal, "agreement", "MEDIUM")
    outlier = getattr(signal, "outlier_dampened", None)
    agreement_icon = {"HIGH": "🟢", "MEDIUM": "🟡", "LOW": "🔴"}.get(agreement, "🟡")
    source_parts = []
    for n in ["gfs", "ecmwf", "gem", "nws"]:
        if n in sp:
            tag = " ⚡" if n == outlier else ""
            source_parts.append(f"{n.upper()}: {sp[n]:.0%}{tag}")
    source_breakdown = "  |  ".join(source_parts) if source_parts else "GFS only"
    if outlier:
        source_breakdown += f"\n*{outlier.upper()} dampened (outlier)*"

    # Show all probabilities from the perspective of the side we're betting.
    # model_probability and market_probability are always P(YES).
    # For NO bets, flip them so the numbers match the side label.
    betting_no = signal.direction == "no"
    display_edge     = abs(signal.edge)
    display_model    = (1.0 - signal.model_probability)    if betting_no else signal.model_probability
    display_market   = (1.0 - signal.market_probability)   if betting_no else signal.market_probability
    prob_label       = f"Our Model P({side})"
    market_label     = f"Market P({side})"

    # Per-source breakdown: also flip to NO side
    source_parts = []
    for n in ["gfs", "ecmwf", "gem", "nws"]:
        if n in sp:
            p = (1.0 - sp[n]) if betting_no else sp[n]
            tag = " ⚡" if n == outlier else ""
            source_parts.append(f"{n.upper()}: {p:.0%}{tag}")
    source_breakdown = "  |  ".join(source_parts) if source_parts else "GFS only"
    if outlier:
        source_breakdown += f"\n*{outlier.upper()} dampened (outlier)*"

    fields = [
        {"name": "Ticker",       "value": f"`{market.market_id}`",          "inline": True},
        {"name": "Side",         "value": f"**{side}**",                     "inline": True},
        {"name": "Edge",         "value": f"**+{display_edge:.1%}**",        "inline": True},
        {"name": prob_label,     "value": f"{display_model:.1%}",            "inline": True},
        {"name": market_label,   "value": f"{display_market:.1%}",           "inline": True},
        {"name": "Kelly Size",   "value": f"${signal.suggested_size:.0f}",   "inline": True},
        {"name": "Contracts",    "value": str(trade.contracts),               "inline": True},
        {"name": "Entry Price",  "value": f"{trade.entry_price:.2%}",        "inline": True},
        {"name": "Agreement",    "value": f"{agreement_icon} {agreement}" + low_conf_note, "inline": True},
        {"name": "Model Breakdown", "value": source_breakdown,               "inline": False},
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



def send_paper_report() -> bool:
    """Post a full paper trading report to Discord on demand."""
    if not settings.DISCORD_WEBHOOK_URL:
        return False

    from backend.core.paper_trading import get_paper_stats, get_model_accuracy

    s = get_paper_stats()
    accuracy = get_model_accuracy()

    total    = s["total"]
    resolved = s["resolved"]
    pending  = s["unresolved"]
    wins     = s["wins"]
    losses   = s["losses"]
    pnl      = s["total_pnl"]
    brier    = s["brier"]
    avg_edge = s["avg_edge"]

    pnl_sign    = "+" if pnl >= 0 else ""
    color       = COLOR_GREEN if pnl >= 0 else COLOR_RED
    win_rate    = f"{wins/(resolved)*100:.0f}%" if resolved > 0 else "n/a"
    brier_str   = f"{brier:.4f}" if brier is not None else "n/a"

    # Active trades list
    active = s["all_trades"]
    pending_trades = [t for t in active if not t.resolved]
    resolved_trades = sorted(
        [t for t in active if t.resolved],
        key=lambda t: t.resolved_at or datetime.utcnow(),
        reverse=True,
    )

    if pending_trades:
        pending_lines = [
            f"`{t.ticker}` {t.side.upper()} edge={t.edge:+.0%} [{getattr(t,'agreement','?')}]"
            for t in pending_trades[:10]
        ]
        if len(pending_trades) > 10:
            pending_lines.append(f"…+{len(pending_trades)-10} more")
        pending_text = "\n".join(pending_lines)
    else:
        pending_text = "None"

    if resolved_trades:
        resolved_lines = []
        for t in resolved_trades[:8]:
            icon = "✅" if t.result == "win" else "❌"
            kalshi_result = "YES" if (t.actual_temp or 0) >= 1.0 else "NO"
            resolved_lines.append(
                f"{icon} `{t.ticker}` Kalshi={kalshi_result}  side={t.side.upper()}  ${t.pnl:+.2f}"
            )
        if len(resolved_trades) > 8:
            resolved_lines.append(f"…+{len(resolved_trades)-8} more")
        resolved_text = "\n".join(resolved_lines)
    else:
        resolved_text = "None resolved yet"

    # Agreement breakdown
    agr = s.get("agreement_levels", {})
    agr_lines = []
    for lvl in ["HIGH", "MEDIUM", "LOW"]:
        if lvl in agr:
            a = agr[lvl]
            sign = "+" if a["pnl"] >= 0 else ""
            agr_lines.append(f"**{lvl}**: {a['wins']}W/{a['losses']}L  {sign}${a['pnl']:.2f}")
    agr_text = "  |  ".join(agr_lines) if agr_lines else "No resolved trades yet"

    # Top model accuracy rows
    if accuracy:
        acc_lines = []
        for r in accuracy[:8]:
            if r["n"] == 0:
                continue
            b = f"{r['brier']:.3f}" if r["brier"] else "n/a"
            acc_lines.append(f"`{r['model']}/{r['city'][:6]}`: Brier={b} ({r['wins']}W/{r['losses']}L)")
        acc_text = "\n".join(acc_lines) if acc_lines else "No settled trades yet"
    else:
        acc_text = "No settled trades yet"

    fields = [
        {"name": "Total Trades",      "value": str(total),                    "inline": True},
        {"name": "Resolved",          "value": str(resolved),                  "inline": True},
        {"name": "Pending",           "value": str(pending),                   "inline": True},
        {"name": "W/L",               "value": f"{wins}W / {losses}L ({win_rate})", "inline": True},
        {"name": "Running P&L",       "value": f"**{pnl_sign}${pnl:.2f}**",  "inline": True},
        {"name": "Avg Edge at Entry", "value": f"{avg_edge:+.1%}",            "inline": True},
        {"name": "Brier Score",       "value": brier_str,                     "inline": True},
        {"name": "Agreement Breakdown", "value": agr_text,                    "inline": False},
        {"name": f"Active Trades ({len(pending_trades)})", "value": pending_text, "inline": False},
        {"name": f"Resolved ({len(resolved_trades)})",     "value": resolved_text, "inline": False},
        {"name": "Model Accuracy",    "value": acc_text,                      "inline": False},
    ]

    embed = {
        "title": "📊 Paper Trading Report",
        "color": color,
        "fields": fields,
        "footer": {"text": "Kalshi Weather Arb Bot · On-Demand Report"},
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    success = _post_embed(embed)
    if success:
        logger.info("Discord paper report sent")
    return success


def poll_discord_commands() -> bool:
    """
    Poll the Discord channel for messages containing 'report' (case-insensitive).
    Posts a full paper report for each matching message, then acknowledges via reaction.

    Requires DISCORD_BOT_TOKEN and DISCORD_CHANNEL_ID in settings.
    Returns True if any commands were processed.
    """
    token      = settings.DISCORD_BOT_TOKEN
    channel_id = settings.DISCORD_CHANNEL_ID
    if not token or not channel_id:
        return False

    headers = {
        "Authorization": f"Bot {token}",
        "Content-Type": "application/json",
    }

    try:
        # Fetch recent messages
        resp = requests.get(
            f"https://discord.com/api/v10/channels/{channel_id}/messages",
            headers=headers,
            params={"limit": 20},
            timeout=10,
        )
        if resp.status_code != 200:
            logger.warning(f"Discord message poll returned {resp.status_code}")
            return False

        messages = resp.json()
        processed = False

        for msg in messages:
            msg_id      = msg.get("id", "")
            content     = msg.get("content", "").strip().lower()
            author      = msg.get("author", {})
            is_bot      = author.get("bot", False)

            if is_bot:
                continue
            if content not in ("report", "!report", "/report"):
                continue

            # Check if already reacted (avoid re-processing)
            reactions = msg.get("reactions", [])
            already_done = any(
                r.get("emoji", {}).get("name") == "✅" and r.get("me", False)
                for r in reactions
            )
            if already_done:
                continue

            # Post the report
            send_paper_report()

            # Add ✅ reaction to mark as handled
            requests.put(
                f"https://discord.com/api/v10/channels/{channel_id}/messages/{msg_id}/reactions/✅/@me",
                headers=headers,
                timeout=10,
            )
            processed = True
            logger.info(f"Processed Discord 'report' command from {author.get('username','?')}")

        return processed

    except Exception as e:
        logger.warning(f"Discord command poll failed: {e}")
        return False


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
