"""Live trading — order placement, settlement, and stats."""
import json
import logging
from datetime import date, datetime
from typing import List, Optional, Tuple

from weatherbot.config import settings
from weatherbot.core.paper_trading import _fetch_kalshi_result
from weatherbot.models.trade import SessionLocal, Trade, init_trade_db

logger = logging.getLogger("weatherbot")


# ── Place a live order ────────────────────────────────────────────────────────

_WEATHER_PREFIXES = ("KXHIGH", "KXLOW", "KXRAIN")


def _is_weather_ticker(ticker: str) -> bool:
    return any(ticker.upper().startswith(p) for p in _WEATHER_PREFIXES)


def _position_orders(trade) -> List[dict]:
    """
    Return the list of orders that built a position as
    [{"id", "price", "n"}, ...].

    New rows store this directly in trade.orders (JSON). Legacy rows predate the
    column, so reconstruct a single-order list from the flat fields.
    """
    if trade.orders:
        try:
            parsed = json.loads(trade.orders)
            if isinstance(parsed, list) and parsed:
                return parsed
        except (ValueError, TypeError):
            logger.warning(f"Bad orders JSON on trade {trade.id} ({trade.ticker}) — using flat fields")
    return [{
        "id":    trade.kalshi_order_id,
        "price": trade.fill_price if trade.fill_price else trade.entry_price,
        "n":     trade.contracts or 0,
    }]


async def log_live_trade(signal) -> Optional[Trade]:
    """
    Place a real Kalshi order and record it as a live Trade (is_paper=False).

    Reconciles against any existing open orders for the same market+side: only
    the gap between contracts already working (filled + resting) and the Kelly
    target is ordered, so partial fills get topped up on later scans instead of
    being left stuck below target until settlement.

    Returns the new Trade row, or None if skipped (already at target, error,
    zero price).
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
    target_contracts = max(1, int(capped_size / entry_price))
    yes_price_cents = round(market.yes_price * 100)  # always YES-side price per Kalshi API

    db = SessionLocal()
    try:
        from weatherbot.data.kalshi_client import KalshiClient
        from weatherbot.notifications.discord import send_live_order_failed_alert
        client = KalshiClient()

        # ── Reconcile against existing orders (top-up logic) ──────────────────
        # Instead of skipping when a pending trade exists, count how many
        # contracts are already working for this market (filled + still resting
        # on the same side) and only order the remaining gap to the Kelly
        # target.  A partial fill on an earlier scan thus gets topped up here
        # rather than left stuck below target until settlement.
        existing = db.query(Trade).filter(
            Trade.ticker == market.market_id,
            Trade.is_paper == False,
            Trade.resolved == False,
            Trade.side == signal.direction,
        ).all()

        # A market+side has at most one open position row now (top-ups fold into
        # it), but query defensively in case legacy data has several.
        already_working = 0
        for ex in existing:
            filled, resting, undetermined, _ = await _position_fill_status(client, ex)
            # Undetermined orders count as working so a transient API hiccup
            # never makes us double-order.
            already_working += filled + resting + undetermined

        contracts = target_contracts - already_working
        if contracts <= 0:
            logger.debug(
                f"Live top-up skipped: {market.market_id} — "
                f"{already_working} contracts already working ≥ target {target_contracts}"
            )
            return None

        # The position row top-ups fold into — the earliest existing order.
        anchor = min(existing, key=lambda ex: ex.created_at or datetime.max) if existing else None

        if existing:
            # ── Top-up guards ─────────────────────────────────────────────────
            # The edge is already re-checked upstream each scan (the signal only
            # reaches here if it still clears MIN_EDGE_THRESHOLD at the current
            # price), so every add is +EV at the moment. That handles the
            # "chasing up" case — when the ask rises toward our view, edge shrinks
            # and the gate eventually blocks it. The dangerous direction is the
            # opposite: when our side's ask FALLS, the market is disagreeing with
            # us harder and Kelly wants to add MORE. Averaging down like that is
            # only safe if our (possibly stale) model still backs the position.
            first_fill = anchor.fill_price or anchor.entry_price

            def _our_conviction(p_yes: float) -> float:
                # Confidence in the side we're actually holding (0..1).
                return p_yes if signal.direction == "yes" else 1.0 - p_yes

            entry_conviction   = _our_conviction(anchor.model_prob or 0.0)
            current_conviction = _our_conviction(signal.model_probability)

            # Guard A — model must still corroborate the position. If our own
            # conviction has weakened since entry, don't add even if a stale edge
            # still clears threshold.
            if current_conviction < entry_conviction:
                logger.info(
                    f"LIVE TOP-UP SKIPPED — model weakened: {market.market_id} "
                    f"{signal.direction.upper()} conviction "
                    f"{current_conviction:.0%} < entry {entry_conviction:.0%}"
                )
                return None

            # Guard B — averaging down. If our ask has fallen more than
            # TOPUP_MAX_ADVERSE_DROP below the original fill, the market has moved
            # against us; only pile in further if the model has actively
            # STRENGTHENED (strictly above entry), not merely held.
            adverse_drop = first_fill - entry_price
            if adverse_drop > settings.TOPUP_MAX_ADVERSE_DROP and current_conviction <= entry_conviction:
                logger.info(
                    f"LIVE TOP-UP SKIPPED — averaging down without conviction: "
                    f"{market.market_id} {signal.direction.upper()} ask "
                    f"{entry_price:.2%} is {adverse_drop:.2%} below first fill "
                    f"{first_fill:.2%}; conviction {current_conviction:.0%} "
                    f"not above entry {entry_conviction:.0%}"
                )
                return None

            logger.info(
                f"LIVE TOP-UP: {market.market_id} {signal.direction.upper()} — "
                f"{already_working} working, target {target_contracts}, "
                f"ordering {contracts} more"
            )

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

        new_order = {"id": order_id, "price": fill_price, "n": contracts}

        if anchor is not None:
            # ── Top-up: fold into the existing position row ───────────────────
            # Blend cost basis by total cost / total contracts. This is exact for
            # P&L (basis * count reproduces total cost); settlement later
            # recomputes the basis from each order's ACTUAL fills.
            prior_cost  = (anchor.contracts or 0) * (anchor.entry_price or 0.0)
            add_cost    = contracts * fill_price
            new_total   = (anchor.contracts or 0) + contracts

            anchor.contracts   = new_total
            anchor.entry_price = round((prior_cost + add_cost) / new_total, 4) if new_total else fill_price
            anchor.fill_price  = anchor.entry_price
            anchor.kelly_size  = round((anchor.kelly_size or 0.0) + order_cost, 2)
            anchor.orders      = json.dumps(_position_orders(anchor) + [new_order])
            # Refresh signal snapshot to the latest scan that justified the add.
            anchor.model_prob    = signal.model_probability
            anchor.market_price  = signal.market_probability
            anchor.edge          = signal.edge
            db.commit()
            db.refresh(anchor)

            anchor.topup_added = contracts   # transient hint for the alert layer
            logger.info(
                f"💸 LIVE TOP-UP logged: {market.market_id}  {signal.direction.upper()}  "
                f"+{contracts} → {new_total} contracts  "
                f"blended @ {anchor.entry_price:.2%}  order_id={order_id}"
            )
            return anchor

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
            kelly_size       = order_cost,   # this order's committed cost (gap only, not full target)
            contracts        = contracts,
            entry_price      = entry_price,
            fill_price       = fill_price,
            kalshi_order_id  = order_id,
            orders           = json.dumps([new_order]),
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
            # ── Step 1: tally ACTUAL fills across every order in the position ──
            total_filled, total_resting, undetermined, per_order = \
                await _position_fill_status(client, trade)

            if total_resting > 0:
                # Some contracts still working — don't settle until they resolve.
                logger.info(
                    f"Skipping {trade.ticker} — {total_resting} contract(s) still resting"
                )
                continue

            if total_filled == 0 and undetermined == 0:
                # Every order cancelled/expired with zero fills — no P&L
                trade.resolved    = True
                trade.result      = "cancelled"
                trade.pnl         = 0.0
                trade.resolved_at = datetime.utcnow()
                settled.append(trade)
                logger.info(f"🚫 LIVE CANCELLED: {trade.ticker} — no order filled")
                continue

            # ── Step 2: fetch the Kalshi market result ─────────────────────
            kalshi_result = await _fetch_kalshi_result(trade.ticker)

            if kalshi_result is None:
                logger.info(f"Skipping {trade.ticker} — Kalshi result not posted yet")
                continue

            # Recompute the blended cost basis from actual fills (ground truth).
            # per_order already falls back to recorded counts for undetermined
            # orders, so this is robust to a stray unfetchable order id.
            cost   = sum(o["filled"] * o["price"] for o in per_order)
            filled_count = sum(o["filled"] for o in per_order)

            if filled_count == 0:
                logger.warning(
                    f"{trade.ticker}: no resolvable fills but status not all-cancelled "
                    f"— settling with recorded contract count {trade.contracts}"
                )
                filled_count = trade.contracts or 0
            else:
                basis = cost / filled_count
                if filled_count != trade.contracts:
                    logger.info(
                        f"Fill reconcile for {trade.ticker}: "
                        f"{filled_count}/{trade.contracts} contracts actually filled, "
                        f"basis {trade.entry_price:.2%}→{basis:.2%}"
                    )
                trade.contracts   = filled_count
                trade.entry_price = round(basis, 4)

            yes_wins = (kalshi_result == "yes")
            we_win = yes_wins if trade.side == "yes" else not yes_wins

            if we_win:
                pnl = (1.0 - trade.entry_price) * filled_count * (1.0 - settings.KALSHI_FEE_RATE)
                result = "win"
            else:
                pnl = trade.entry_price * filled_count * -1.0
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


async def _fetch_order_fill(client, order_id, fallback_n) -> Optional[Tuple[int, int]]:
    """
    Resolve a single Kalshi order to (filled_contracts, resting_contracts), or
    None if the status couldn't be determined (transient API error / unknown
    status). `fallback_n` is used when there's no order id, or a 'filled' order
    reports no count.
    """
    if not order_id:
        # No order id recorded — assume the recorded size fully filled.
        return (fallback_n, 0)
    try:
        data = await client.get_order(order_id)
        order = data.get("order", data)
        status = order.get("status", "")

        # Derive filled count from order fields.
        # Kalshi may return filled_count directly, or we compute it from count - remaining_count.
        count     = int(order.get("count", 0) or 0)
        remaining = int(order.get("remaining_count", 0) or 0)
        filled_direct = int(
            order.get("filled_count", 0) or order.get("count_filled", 0) or 0
        )
        filled_count = filled_direct if filled_direct > 0 else max(0, count - remaining)

        # Kalshi order statuses: "resting", "filled", "canceled"/"cancelled", "expired"
        if status == "filled":
            return (filled_count if filled_count > 0 else fallback_n, 0)

        if status in ("cancelled", "canceled", "expired"):
            # Dead order — any fill is final, nothing left resting.
            return (filled_count, 0)

        if status == "resting":
            # Still open: filled_count may be a partial fill so far; the rest
            # is still working on the book. Derive the resting amount from the
            # order's own count so a missing remaining_count field can't make a
            # live order look fully filled (which would settle it prematurely).
            resting = max(remaining, count - filled_count)
            return (filled_count, max(0, resting))

        # Unrecognised status — treat as undetermined.
        return None
    except Exception as e:
        logger.debug(f"Could not fetch order status for {order_id}: {e}")
        return None


async def _position_fill_status(client, trade) -> Tuple[int, int, int, List[dict]]:
    """
    Aggregate fill status across every order that built a position.

    Returns (total_filled, total_resting, undetermined, per_order):
      - total_filled:  contracts actually traded across all orders
      - total_resting: contracts still open on the book (can still fill)
      - undetermined:  recorded contracts whose status couldn't be fetched
      - per_order:     [{"price", "filled"} ...] for cost-basis recompute.
                       Undetermined orders fall back to their recorded count so
                       settlement can still compute a basis rather than block.
    """
    total_filled = total_resting = undetermined = 0
    per_order: List[dict] = []
    for o in _position_orders(trade):
        n     = int(o.get("n", 0) or 0)
        price = o.get("price")
        res = await _fetch_order_fill(client, o.get("id"), n)
        if res is None:
            # Status unknown — fall back to the recorded fill so the basis math
            # still works, and flag it so the reconcile path stays conservative.
            undetermined += n
            if n > 0 and price is not None:
                per_order.append({"price": price, "filled": n})
            continue
        filled, resting = res
        total_filled  += filled
        total_resting += resting
        if filled > 0 and price is not None:
            per_order.append({"price": price, "filled": filled})
    return (total_filled, total_resting, undetermined, per_order)


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
