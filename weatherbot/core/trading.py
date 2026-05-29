"""Live trading — order placement, settlement, and stats."""
import json
import logging
from datetime import date, datetime
from typing import List, Optional

from weatherbot.config import settings
from weatherbot.core.paper_trading import _fetch_kalshi_result
from weatherbot.models.trade import SessionLocal, Trade, init_trade_db

logger = logging.getLogger("weatherbot")


# ── Place a live order ────────────────────────────────────────────────────────

_WEATHER_PREFIXES = ("KXHIGH", "KXLOW", "KXRAIN")


def _is_weather_ticker(ticker: str) -> bool:
    return any(ticker.upper().startswith(p) for p in _WEATHER_PREFIXES)


async def log_live_trade(signal) -> Optional[Trade]:
    """
    Place a real Kalshi order and record it as a live Trade (is_paper=False).
    Returns the Trade row, or None if skipped (dedup, error, zero price).
    """
    init_trade_db()

    market = signal.market

    # Hard guardrail: only ever trade weather markets
    if not _is_weather_ticker(market.market_id):
        logger.error(
            f"NON-WEATHER TICKER BLOCKED: {market.market_id} — "
            f"only KXHIGH/KXLOW/KXRAIN markets are allowed"
        )
        return None

    entry_price = market.yes_price if signal.direction == "yes" else market.no_price
    if entry_price <= 0:
        return None

    capped_size = min(signal.suggested_size, settings.LIVE_MAX_TRADE_SIZE)
    contracts = max(1, int(capped_size / entry_price))
    yes_price_cents = round(market.yes_price * 100)  # always YES-side price per Kalshi API

    db = SessionLocal()
    try:
        existing = db.query(Trade).filter(
            Trade.ticker == market.market_id,
            Trade.is_paper == False,
            Trade.resolved == False,
        ).first()
        if existing:
            logger.debug(f"Live dedup skipped: {market.market_id} (already pending)")
            return None

        from weatherbot.data.kalshi_client import KalshiClient
        from weatherbot.notifications.discord import send_live_order_failed_alert
        client = KalshiClient()

        # ── Balance preflight ─────────────────────────────────────────────
        # Guard 1: local — if capped_size can't cover even 1 contract, skip.
        # Guard 2: API — confirm Kalshi balance covers the order cost.
        if capped_size < entry_price:
            reason = (
                f"Sized out: Kelly size ${capped_size:.2f} < "
                f"1-contract cost ${entry_price:.2f} for {market.market_id}"
            )
            logger.warning(f"LIVE ORDER SKIPPED — {reason}")
            send_live_order_failed_alert(market.market_id, reason)
            return None

        order_cost = round(contracts * entry_price, 2)
        try:
            balance_data = await client.get_balance()
            # Kalshi returns `balance` in cents; `balance_dollars` is the string equivalent
            if "balance_dollars" in balance_data:
                available_dollars = float(balance_data["balance_dollars"])
            else:
                available_dollars = balance_data.get("balance", 0) / 100.0
            if available_dollars < order_cost:
                reason = (
                    f"Insufficient funds: ${available_dollars:.2f} available, "
                    f"${order_cost:.2f} required ({contracts} contracts @ {entry_price:.2%})"
                )
                logger.error(f"LIVE ORDER SKIPPED {market.market_id} — {reason}")
                send_live_order_failed_alert(market.market_id, reason)
                return None
        except Exception as balance_err:
            logger.warning(
                f"Balance preflight check failed for {market.market_id}: {balance_err} "
                f"— proceeding with order attempt"
            )

        logger.info(
            f"LIVE ORDER: {market.market_id}  {signal.direction.upper()}  "
            f"{contracts} contracts @ {entry_price:.2%}  "
            f"(capped ${capped_size:.2f} of ${signal.suggested_size:.2f} Kelly)"
        )

        result = await client.place_order(
            ticker=market.market_id,
            side=signal.direction,
            count=contracts,
            yes_price=yes_price_cents,
        )

        order = result.get("order", result)
        order_id = order.get("id") or order.get("order_id")
        fill_price_raw = order.get("yes_price") or order.get("fill_price")
        fill_price = (fill_price_raw / 100.0) if fill_price_raw else entry_price

        trade = Trade(
            is_paper         = False,
            ticker           = market.market_id,
            city             = market.city_key,
            metric           = market.metric,
            threshold_f      = market.threshold_f,
            side             = signal.direction,
            market_direction = market.direction,
            agreement        = getattr(signal, "agreement", "MEDIUM"),
            model_probs      = json.dumps(getattr(signal, "source_probs", {})),
            model_prob       = signal.model_probability,
            market_price     = signal.market_probability,
            edge             = signal.edge,
            confidence       = signal.confidence,
            kelly_size       = capped_size,
            contracts        = contracts,
            entry_price      = entry_price,
            fill_price       = fill_price,
            kalshi_order_id  = order_id,
            forecast_mean    = signal.ensemble_mean,
            forecast_std     = signal.ensemble_std,
            resolution_date  = market.target_date.isoformat(),
            resolved         = False,
        )
        db.add(trade)
        db.commit()
        db.refresh(trade)

        logger.info(
            f"💸 LIVE TRADE logged: {market.market_id}  {signal.direction.upper()}  "
            f"{contracts} contracts @ {fill_price:.2%}  order_id={order_id}"
        )
        return trade

    except Exception as e:
        logger.error(f"Live order failed for {market.market_id}: {e}", exc_info=True)
        db.rollback()
        try:
            from weatherbot.notifications.discord import send_live_order_failed_alert
            send_live_order_failed_alert(market.market_id, f"Order error: {e}")
        except Exception:
            pass
        return None
    finally:
        db.close()


# ── Settlement ────────────────────────────────────────────────────────────────

async def settle_live_trades() -> List[Trade]:
    """
    Settle live trades whose resolution date has passed.

    Unlike paper settlement, we first check whether the order actually filled.
    Unfilled orders (expired/cancelled) are marked 'cancelled' with pnl=0.
    Filled orders are settled via the Kalshi market result.
    """
    init_trade_db()
    today = date.today()
    settled = []

    db = SessionLocal()
    try:
        pending = db.query(Trade).filter(
            Trade.is_paper == False,
            Trade.resolved == False,
            Trade.resolution_date <= today.isoformat(),
        ).all()

        if not pending:
            logger.debug("No live trades ready for settlement")
            return []

        logger.info(f"Settling {len(pending)} live trade(s)...")

        from weatherbot.data.kalshi_client import KalshiClient
        client = KalshiClient()

        for trade in pending:
            # ── Step 1: check if the order filled ─────────────────────────
            filled = await _check_order_filled(client, trade)

            if filled is None:
                logger.info(f"Skipping {trade.ticker} — could not fetch order status")
                continue

            if not filled:
                # Order expired or was cancelled without filling — no P&L
                trade.resolved    = True
                trade.result      = "cancelled"
                trade.pnl         = 0.0
                trade.resolved_at = datetime.utcnow()
                settled.append(trade)
                logger.info(f"🚫 LIVE CANCELLED: {trade.ticker} — order did not fill")
                continue

            # ── Step 2: fetch the Kalshi market result ─────────────────────
            kalshi_result = await _fetch_kalshi_result(trade.ticker)

            if kalshi_result is None:
                logger.info(f"Skipping {trade.ticker} — Kalshi result not posted yet")
                continue

            yes_wins = (kalshi_result == "yes")
            we_win = yes_wins if trade.side == "yes" else not yes_wins

            if we_win:
                pnl = (1.0 - trade.entry_price) * trade.contracts * (1.0 - settings.KALSHI_FEE_RATE)
                result = "win"
            else:
                pnl = trade.entry_price * trade.contracts * -1.0
                result = "loss"

            trade.resolved    = True
            trade.result      = result
            trade.pnl         = round(pnl, 2)
            trade.actual_temp = 1.0 if yes_wins else 0.0
            trade.resolved_at = datetime.utcnow()
            settled.append(trade)

            icon = "✅" if result == "win" else "❌"
            logger.info(
                f"{icon} LIVE SETTLED: {trade.ticker}  {result.upper()}  "
                f"Kalshi={kalshi_result.upper()}  side={trade.side.upper()}  "
                f"P&L=${pnl:+.2f}"
            )

        db.commit()

    except Exception as e:
        logger.error(f"Live trade settlement error: {e}", exc_info=True)
        db.rollback()
    finally:
        db.close()

    return settled


async def _check_order_filled(client, trade) -> Optional[bool]:
    """
    Returns True if the order filled, False if it expired/cancelled unfilled,
    None if the order status couldn't be fetched.
    """
    if not trade.kalshi_order_id:
        # No order ID recorded — assume filled (best we can do)
        return True
    try:
        data = await client.get_order(trade.kalshi_order_id)
        order = data.get("order", data)
        status = order.get("status", "")
        # Kalshi order statuses: "resting", "filled", "cancelled", "expired"
        if status == "filled":
            return True
        if status in ("cancelled", "expired"):
            return False
        # "resting" means still open — resolution date passed but order not resolved yet
        return None
    except Exception as e:
        logger.debug(f"Could not fetch order status for {trade.kalshi_order_id}: {e}")
        return None


# ── Stats ─────────────────────────────────────────────────────────────────────

def get_live_stats(db=None) -> dict:
    """Return aggregate stats for live (real money) trades."""
    init_trade_db()
    close_after = db is None
    if db is None:
        db = SessionLocal()

    try:
        all_trades = db.query(Trade).filter(Trade.is_paper == False).all()
        resolved   = [t for t in all_trades if t.resolved]
        unresolved = [t for t in all_trades if not t.resolved]
        wins       = [t for t in resolved if t.result == "win"]
        losses     = [t for t in resolved if t.result == "loss"]
        cancelled  = [t for t in resolved if t.result == "cancelled"]
        total_pnl  = sum(t.pnl for t in resolved if t.pnl is not None)
        avg_edge   = (sum(t.edge for t in all_trades) / len(all_trades)) if all_trades else 0.0

        return {
            "total":      len(all_trades),
            "resolved":   len(resolved),
            "unresolved": len(unresolved),
            "wins":       len(wins),
            "losses":     len(losses),
            "cancelled":  len(cancelled),
            "total_pnl":  total_pnl,
            "avg_edge":   avg_edge,
            "all_trades": all_trades,
            "resolved_trades": resolved,
        }
    finally:
        if close_after:
            db.close()
