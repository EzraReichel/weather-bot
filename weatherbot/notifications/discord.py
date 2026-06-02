"""Discord webhook notifications for weather arb signals."""
import logging
import time
from datetime import datetime, timezone
from typing import List, Optional

import requests

from weatherbot.config import settings

logger = logging.getLogger("weatherbot")

# Discord embed color constants
COLOR_GREEN = 0x2ECC71   # Actionable signal
COLOR_YELLOW = 0xF39C12  # Low-confidence signal
COLOR_BLUE = 0x3498DB    # Daily summary
COLOR_RED = 0xE74C3C     # Error/alert

# Discord hard limits — exceeding any of these makes the API reject the whole
# webhook with a 400, so the notification silently never arrives. We clamp
# everything defensively before sending.
_FIELD_NAME_MAX  = 256
_FIELD_VALUE_MAX = 1024
_TITLE_MAX       = 256
_DESC_MAX        = 4096
_MAX_FIELDS      = 25
_ZERO_WIDTH      = "​"   # Discord rejects empty field name/value strings


def _truncate(text, limit: int) -> str:
    """Clamp a string to `limit` chars, appending an ellipsis when cut."""
    s = "" if text is None else str(text)
    if len(s) <= limit:
        return s
    return s[: max(0, limit - 1)] + "…"


def _sanitize_embed(embed: dict) -> dict:
    """
    Clamp every field of an embed to Discord's limits so a single oversized
    value (e.g. a long resolved-trades list) can't cause the entire webhook to
    be rejected with a 400. Mutates and returns a copy.
    """
    e = dict(embed)
    if "title" in e:
        e["title"] = _truncate(e["title"], _TITLE_MAX)
    if "description" in e:
        e["description"] = _truncate(e["description"], _DESC_MAX)

    fields = e.get("fields")
    if fields:
        clamped = []
        for f in fields[:_MAX_FIELDS]:
            clamped.append({
                **f,
                "name":  _truncate(f.get("name", ""),  _FIELD_NAME_MAX)  or _ZERO_WIDTH,
                "value": _truncate(f.get("value", ""), _FIELD_VALUE_MAX) or _ZERO_WIDTH,
            })
        e["fields"] = clamped
    return e


def _post_embed(embed: dict) -> bool:
    """
    POST a single embed to Discord. Returns True on success.

    Clamps the embed to Discord's size limits first, and retries on 429 rate
    limits (honoring retry_after) so bursts of settlement alerts don't get
    silently dropped.
    """
    url = settings.DISCORD_WEBHOOK_URL
    if not url:
        return False

    payload = {"embeds": [_sanitize_embed(embed)]}

    for attempt in range(3):
        try:
            resp = requests.post(url, json=payload, timeout=10)
            if resp.status_code in (200, 204):
                return True
            if resp.status_code == 429:
                retry_after = 1.0
                try:
                    retry_after = float(resp.json().get("retry_after", 1.0))
                except Exception:
                    pass
                retry_after = min(max(retry_after, 0.5), 5.0)
                logger.warning(
                    f"Discord rate limited (429); retrying in {retry_after:.1f}s "
                    f"(attempt {attempt + 1}/3)"
                )
                time.sleep(retry_after)
                continue
            logger.warning(f"Discord webhook returned {resp.status_code}: {resp.text[:300]}")
            return False
        except Exception as e:
            logger.error(f"Discord webhook failed: {e}")
            return False

    logger.warning("Discord webhook failed after retries (rate limited)")
    return False



def send_daily_summary(
    logged_today: list,
    resolved_today: list,
    daily_pnl: float,
    stats: dict,
    daily_brier: Optional[float] = None,
    bankroll: Optional[float] = None,
    live_trading: bool = False,
) -> bool:
    """
    Send combined end-of-day summary to Discord at 11 PM ET.

    Reports on the *active* trading mode only (live when LIVE_TRADING is on,
    otherwise paper) so P&L, counts, and bankroll stay internally consistent —
    no mixing of historical paper P&L into a live running total.
    """
    if not settings.DISCORD_WEBHOOK_URL:
        return False

    mode_label = "Live" if live_trading else "Paper"

    pnl_sign = "+" if daily_pnl >= 0 else ""
    running_pnl = stats.get("total_pnl", 0.0)
    running_sign = "+" if running_pnl >= 0 else ""
    color = COLOR_GREEN if running_pnl >= 0 else COLOR_RED
    if bankroll is None:
        bankroll = settings.INITIAL_BANKROLL + running_pnl

    wins = stats.get("wins", 0)
    losses = stats.get("losses", 0)

    # All-time Brier computed generically from resolved trades so it works for
    # both paper and live stats dicts (live stats carries no precomputed brier).
    resolved_trades = stats.get("resolved_trades", []) or []
    brier_scores = [
        (t.model_prob - (1.0 if (t.actual_temp or 0) >= 1.0 else 0.0)) ** 2
        for t in resolved_trades
        if t.model_prob is not None
    ]
    brier = (sum(brier_scores) / len(brier_scores)) if brier_scores else None
    brier_str = f"{brier:.3f}" if brier is not None else "n/a"
    daily_brier_str = f"{daily_brier:.3f}" if daily_brier is not None else "n/a"

    # Trades logged today — brief list (capped to stay under the 1024-char field limit)
    if logged_today:
        logged_lines = [
            f"`{t.ticker}` — {t.side.upper()} edge={t.edge:+.1%} @ {t.entry_price:.2%}"
            for t in logged_today[:12]
        ]
        if len(logged_today) > 12:
            logged_lines.append(f"…+{len(logged_today) - 12} more")
        logged_text = "\n".join(logged_lines)
    else:
        logged_text = "None today"

    # Trades resolved today (capped)
    if resolved_today:
        resolved_lines = []
        for t in resolved_today[:12]:
            icon = "✅" if t.result == "win" else ("🚫" if t.result == "cancelled" else "❌")
            kalshi_result = "YES" if (t.actual_temp or 0) >= 1.0 else "NO"
            pnl_val = t.pnl if t.pnl is not None else 0.0
            resolved_lines.append(
                f"{icon} `{t.ticker}` Kalshi={kalshi_result}  side={t.side.upper()}  ${pnl_val:+.2f}"
            )
        if len(resolved_today) > 12:
            resolved_lines.append(f"…+{len(resolved_today) - 12} more")
        resolved_text = "\n".join(resolved_lines)
    else:
        resolved_text = "None settled today"

    fields = [
        {"name": f"{mode_label} Trades Today", "value": str(len(logged_today)), "inline": True},
        {"name": "Today's P&L",          "value": f"**{pnl_sign}${daily_pnl:.2f}**", "inline": True},
        {"name": f"{mode_label} P&L (all-time)", "value": f"**{running_sign}${running_pnl:.2f}**", "inline": True},
        {"name": "Bankroll",             "value": f"**${bankroll:,.2f}**",                 "inline": True},
        {"name": "All-time W/L",         "value": f"{wins}W / {losses}L",     "inline": True},
        {"name": "Brier (today)",        "value": daily_brier_str,             "inline": True},
        {"name": "Brier (all-time)",     "value": brier_str,                   "inline": True},
        {"name": f"New {mode_label} Trades", "value": logged_text,             "inline": False},
        {"name": "Resolved Today",       "value": resolved_text,               "inline": False},
    ]

    embed = {
        "title": f"📊 Daily Summary · {mode_label}",
        "color": color,
        "fields": fields,
        "footer": {"text": f"Kalshi Weather Arb Bot · {mode_label} · 11 PM ET"},
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    success = _post_embed(embed)
    if success:
        logger.info("Discord daily summary sent")
    return success


def send_trade_settled_alert(trade, bankroll: Optional[float] = None) -> bool:
    """Send a Discord alert when a trade resolves (win or loss)."""
    if not settings.DISCORD_WEBHOOK_URL:
        return False

    icon = "✅" if trade.result == "win" else "❌"
    kalshi_result = "YES" if (trade.actual_temp or 0) >= 1.0 else "NO"
    pnl_sign = "+" if (trade.pnl or 0) >= 0 else ""
    is_live = not getattr(trade, "is_paper", True)
    color = COLOR_GREEN if trade.result == "win" else COLOR_RED
    mode_prefix = "💸 LIVE" if is_live else "📝 PAPER"

    yes_outcome = 1.0 if (trade.actual_temp or 0) >= 1.0 else 0.0
    brier_contrib = (trade.model_prob - yes_outcome) ** 2

    fields = [
        {"name": "Ticker",            "value": f"`{trade.ticker}`",                       "inline": True},
        {"name": "Side",              "value": f"**{trade.side.upper()}**",                "inline": True},
        {"name": "Result",            "value": f"{icon} **{trade.result.upper()}**",       "inline": True},
        {"name": "Kalshi Outcome",    "value": kalshi_result,                              "inline": True},
        {"name": "Entry Price",       "value": f"{trade.entry_price:.2%}",                 "inline": True},
        {"name": "P&L",               "value": f"**{pnl_sign}${trade.pnl:.2f}**",         "inline": True},
        {"name": "Model Prob (YES)",  "value": f"{trade.model_prob:.1%}",                  "inline": True},
        {"name": "Edge at Entry",     "value": f"{trade.edge:+.1%}",                       "inline": True},
        {"name": "Brier (this trade)","value": f"{brier_contrib:.3f}",                     "inline": True},
    ]
    if bankroll is not None:
        fields.append({"name": "Bankroll", "value": f"**${bankroll:,.2f}**", "inline": True})

    embed = {
        "title": f"{icon} {mode_prefix} SETTLED — {trade.ticker}",
        "description": f"**{trade.city.upper()}** · {trade.metric.upper()} · {trade.threshold_f:.0f}°F · {trade.resolution_date}",
        "color": color,
        "fields": fields,
        "footer": {"text": f"Kalshi Weather Arb Bot · {'Live' if is_live else 'Paper'} Settlement"},
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    success = _post_embed(embed)
    if success:
        logger.info(f"Discord settlement alert sent: {trade.ticker} {trade.result}")
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

    # Show all probabilities from the perspective of the side we're betting.
    # model_probability and market_probability are always P(YES).
    # For NO bets, flip them so the numbers match the side label.
    betting_no = signal.direction == "no"
    display_edge     = abs(signal.edge)
    display_model    = (1.0 - signal.model_probability)    if betting_no else signal.model_probability
    display_market   = (1.0 - signal.market_probability)   if betting_no else signal.market_probability
    prob_label       = f"Our Model P({side})"
    market_label     = f"Market P({side})"

    # Per-source breakdown: flip to NO side when betting NO
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



def send_paper_report(bankroll: Optional[float] = None) -> bool:
    """Post a full paper trading report to Discord on demand."""
    if not settings.DISCORD_WEBHOOK_URL:
        return False

    from weatherbot.core.paper_trading import get_paper_stats, get_model_accuracy

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
    if bankroll is None:
        bankroll = settings.INITIAL_BANKROLL + pnl
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
        {"name": "Bankroll",          "value": f"**${bankroll:,.2f}**",       "inline": True},
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


def poll_discord_commands(bankroll: Optional[float] = None) -> bool:
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
            send_paper_report(bankroll=bankroll)

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


def send_live_order_failed_alert(ticker: str, reason: str) -> bool:
    """Send a Discord alert when a live order fails to place (e.g. insufficient funds)."""
    if not settings.DISCORD_WEBHOOK_URL:
        return False

    embed = {
        "title": f"🚨 LIVE ORDER FAILED — {ticker}",
        "description": reason,
        "color": COLOR_RED,
        "footer": {"text": "Kalshi Weather Arb Bot · Live Trading Error"},
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    success = _post_embed(embed)
    if success:
        logger.info(f"Discord live order failure alert sent: {ticker}")
    return success


def send_live_trade_alert(signal, trade) -> bool:
    """
    Send a Discord alert for a live order placement.
    Uses red/orange color so it's visually distinct from paper trade alerts.
    """
    if not settings.DISCORD_WEBHOOK_URL:
        return False

    market = signal.market
    side = signal.direction.upper()
    betting_no = signal.direction == "no"
    display_model  = (1.0 - signal.model_probability)  if betting_no else signal.model_probability

    entry_price = trade.entry_price
    fill_value = trade.fill_price if trade.fill_price else trade.entry_price
    fill_str = f"{fill_value * 100:.0f}¢"
    order_id_str = trade.kalshi_order_id or "unknown"

    embed = {
        "title": f"💸 LIVE ORDER PLACED — {market.market_id}",
        "color": 0xE67E22,  # orange — distinct from paper (green) and errors (red)
        "fields": [
            {"name": "Ticker",       "value": f"`{market.market_id}`",            "inline": True},
            {"name": "Side",         "value": f"**{side}**",                       "inline": True},
            {"name": "Edge",         "value": f"**+{abs(signal.edge):.1%}**",      "inline": True},
            {"name": "Our Model",    "value": f"{display_model:.1%}",              "inline": True},
            {"name": "Entry Price",  "value": f"{entry_price * 100:.0f}¢",         "inline": True},
            {"name": "Fill Price",   "value": fill_str,                            "inline": True},
            {"name": "Contracts",    "value": str(trade.contracts),                "inline": True},
            {"name": "Size",         "value": f"${trade.kelly_size:.2f}",          "inline": True},
            {"name": "Order ID",     "value": f"`{order_id_str}`",                 "inline": True},
            {"name": "City",         "value": market.city_name,                    "inline": True},
            {"name": "Resolves",     "value": market.target_date.isoformat(),      "inline": True},
            {"name": "Threshold",    "value": f"{market.threshold_f:.0f}°F {market.direction}", "inline": True},
        ],
        "footer": {"text": "⚠️ LIVE TRADING — real money"},
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    success = _post_embed(embed)
    if success:
        logger.info(f"Discord live trade alert sent: {trade.ticker} {side}")
    return success


def send_live_position_increase_alert(signal, trade, added_contracts: int) -> bool:
    """
    Send a Discord alert when a top-up increases an existing live position.

    Distinct from send_live_trade_alert: this is a position-increase update, not
    a brand-new position. Shows how many contracts were added and the new blended
    cost basis on the consolidated row.
    """
    if not settings.DISCORD_WEBHOOK_URL:
        return False

    market = signal.market
    side = signal.direction.upper()
    blended = trade.entry_price or signal.market_probability
    prior_contracts = max(0, (trade.contracts or 0) - added_contracts)

    embed = {
        "title": f"➕ LIVE POSITION INCREASED — {market.market_id}",
        "color": 0xD4AC0D,  # darker gold — distinct from a fresh order (orange)
        "fields": [
            {"name": "Ticker",        "value": f"`{market.market_id}`",                 "inline": True},
            {"name": "Side",          "value": f"**{side}**",                           "inline": True},
            {"name": "Edge",          "value": f"**+{abs(signal.edge):.1%}**",          "inline": True},
            {"name": "Added",         "value": f"+{added_contracts} contracts",         "inline": True},
            {"name": "Position",      "value": f"{prior_contracts} → {trade.contracts}", "inline": True},
            {"name": "Blended Basis", "value": f"{blended:.2%}",                        "inline": True},
            {"name": "Total Cost",    "value": f"${trade.kelly_size:.2f}",              "inline": True},
            {"name": "City",          "value": market.city_name,                        "inline": True},
            {"name": "Resolves",      "value": market.target_date.isoformat(),          "inline": True},
        ],
        "footer": {"text": "⚠️ LIVE TRADING — real money"},
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    success = _post_embed(embed)
    if success:
        logger.info(f"Discord position-increase alert sent: {trade.ticker} {side} +{added_contracts}")
    return success
