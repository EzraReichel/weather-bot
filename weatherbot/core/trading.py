"""Live trading — order placement, settlement, and stats."""
import json
import logging
from datetime import date, datetime, timedelta, timezone
from typing import List, Optional, Tuple

from weatherbot.config import settings
from weatherbot.core.paper_trading import _fetch_kalshi_result, is_warm_outlook
from weatherbot.core.probability import kalshi_trade_fee_ceil, settlement_pnl
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

    Tops up a partially-filled position toward the Kelly target on later scans,
    folding each add into one position row — but never re-buys a position that
    has already closed out. See the reconciliation block below for the exact
    cases and the runaway guard.

    Returns the Trade row (new or topped-up), or None if skipped (at target,
    closed already, opposing side open, guard tripped, error, zero price).
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
    # Limit price for the side we're actually buying: entry_price is already the
    # YES ask for YES orders and the NO ask for NO orders. Sending the YES ask on
    # a NO order would imply a NO limit a full spread too low → never fills.
    limit_price_cents = round(entry_price * 100)

    db = SessionLocal()
    try:
        from weatherbot.data.kalshi_client import KalshiClient
        from weatherbot.notifications.discord import send_live_order_failed_alert
        client = KalshiClient()

        # ── Reconcile against existing rows for this market ───────────────────
        # The ticker is date+threshold specific (e.g. KXHIGHDEN-26JUN02-T79), so
        # every row here is the SAME market today. Three cases:
        #
        #   • Open position on our side  → TOP-UP: order only the remaining gap
        #     to the Kelly target and fold it into that row.
        #   • Only CLOSED rows on our side (filled-and-settled OR cancelled) →
        #     the day's attempt already ended; do NOT re-enter. Re-buying a
        #     market whose order cancelled is the original "runaway re-buy".
        #   • Open position on the OPPOSITE side → skip; never hold both sides.
        rows = db.query(Trade).filter(
            Trade.ticker == market.market_id,
            Trade.is_paper == False,
        ).order_by(Trade.created_at).all()
        open_rows = [r for r in rows if not r.resolved]

        if any(r.side != signal.direction for r in open_rows):
            logger.debug(
                f"Live skip: {market.market_id} — open opposite-side position; "
                f"signal is {signal.direction}, not opening a conflicting side"
            )
            return None

        same_side      = [r for r in rows if r.side == signal.direction]
        same_side_open = [r for r in open_rows if r.side == signal.direction]

        if same_side and not same_side_open:
            logger.debug(
                f"Live skip: {market.market_id} {signal.direction} — prior "
                f"attempt(s) already closed ({len(same_side)} row(s)); not "
                f"re-entering the same market today"
            )
            return None

        anchor = same_side_open[0] if same_side_open else None

        if anchor is None:
            # ── Cross-bracket position guard (fresh entries only) ─────────────
            # The same-ticker reconciliation above only sees THIS date+threshold
            # market. Across scans, live could otherwise stack correlated
            # positions on different brackets of the same city/day/metric, or
            # hold contradictory warm/cold bets. Mirror paper's contradiction
            # guard against every unresolved live position for this city/date:
            #   • opposite warm/cold outlook  → skip (never hold both).
            #   • same metric, same outlook, different ticker → skip: one live
            #     position per city/date/metric bracket.
            # Top-ups (anchor is not None) are exempt — they only add to the
            # position that already passed this guard when it was opened.
            new_is_warm = is_warm_outlook(market.direction, signal.direction)
            siblings = db.query(Trade).filter(
                Trade.is_paper == False,
                Trade.resolved == False,
                Trade.city == market.city_key,
                Trade.resolution_date == market.target_date.isoformat(),
            ).all()
            for prior in siblings:
                prior_is_warm = is_warm_outlook(prior.market_direction, prior.side)
                if prior_is_warm != new_is_warm:
                    logger.info(
                        f"Live skip: {market.market_id} "
                        f"({'warm' if new_is_warm else 'cold'}) contradicts open "
                        f"{prior.ticker} ({'warm' if prior_is_warm else 'cold'}) "
                        f"for {market.city_key} on {market.target_date.isoformat()}"
                    )
                    return None
                if prior.metric == market.metric and prior.ticker != market.market_id:
                    logger.info(
                        f"Live skip: {market.market_id} — already hold "
                        f"{prior.ticker} (same city/date/metric, different "
                        f"bracket); one live position per "
                        f"{market.city_key}/{market.target_date.isoformat()}/{market.metric}"
                    )
                    return None

            # Fresh entry — order the full Kelly target.
            contracts = target_contracts
        else:
            # ── TOP-UP — fill-aware, self-bounding, with a circuit breaker ────
            # Refill toward the Kelly target based on what's ACTUALLY working on
            # Kalshi: filled + still-resting + undetermined. The gap below is
            # `target - working`, which is INHERENTLY self-bounding — contracts
            # filled can never exceed the target, because a cancelled resting
            # order reopens only its own unfilled remainder, never the whole
            # target. So a partial fill climbs to target over later scans without
            # ever over-buying.
            #
            # Why this is safe where the original (395-contract) blowup was not:
            # that version under-counted `working` — a cancelled resting order
            # collapsed the count toward zero, so the gap reopened to the FULL
            # target every scan and it re-bought repeatedly. Two things prevent
            # that here:
            #   • `undetermined` (status-fetch failures) counts as working, so a
            #     transient API error can never make the position look empty and
            #     trigger a re-buy.
            #   • TOPUP_MAX_ORDERS is a hard circuit breaker on ladder length —
            #     even if fill status were ever misread, the order ladder cannot
            #     grow past N, capping worst-case exposure instead of spiraling.
            filled, resting, undetermined, _ = await _position_fill_status(client, anchor)
            working  = filled + resting + undetermined
            n_orders = sum(len(_position_orders(r)) for r in same_side_open)

            # Hard circuit breaker — at most TOPUP_MAX_ORDERS orders per position.
            if n_orders >= settings.TOPUP_MAX_ORDERS:
                logger.info(
                    f"LIVE TOP-UP SKIPPED — order cap: {market.market_id} "
                    f"{signal.direction.upper()} has {n_orders} orders "
                    f"(max {settings.TOPUP_MAX_ORDERS})"
                )
                return None

            # Self-bounding fill gap, capped by the REMAINING DOLLAR BUDGET — not
            # by a contract count recomputed at the current price. target_contracts
            # is int(capped_size / entry_price), which GROWS as the price falls; if
            # we sized the gap off that alone, every downtick would reopen room to
            # buy more, because the contracts already held get re-valued at the new
            # (lower) price even though they were bought higher. That let total
            # dollars climb past the cap — a $100 cap could spend ~$200 as the price
            # halved. Budget in dollars instead: the cap applies to the WHOLE
            # position, so a top-up may spend at most what's left of capped_size
            # after the dollars already committed (anchor.kelly_size).
            spent = anchor.kelly_size or 0.0
            remaining_budget = capped_size - spent
            gap = min(target_contracts - working, int(remaining_budget / entry_price))
            if gap <= 0:
                logger.debug(
                    f"Live top-up skipped: {market.market_id} — working={working}/"
                    f"{target_contracts} contracts, ${spent:.2f}/${capped_size:.2f} "
                    f"budget spent (dollar cap or contract target reached)"
                )
                return None

            # ── Conviction guards (adds only) ─────────────────────────────────
            first_fill = anchor.fill_price or anchor.entry_price

            def _our_conviction(p_yes: float) -> float:
                # Confidence in the side we actually hold (0..1).
                return p_yes if signal.direction == "yes" else 1.0 - p_yes

            entry_conviction   = _our_conviction(anchor.model_prob or 0.0)
            current_conviction = _our_conviction(signal.model_probability)

            # Guard A — don't add if our own conviction has weakened since entry,
            # even if a stale edge still clears threshold.
            if current_conviction < entry_conviction:
                logger.info(
                    f"LIVE TOP-UP SKIPPED — model weakened: {market.market_id} "
                    f"{signal.direction.upper()} conviction {current_conviction:.0%} "
                    f"< entry {entry_conviction:.0%}"
                )
                return None

            # Guard B — averaging down. If our ask fell more than
            # TOPUP_MAX_ADVERSE_DROP below the first fill, only add if the model
            # has actively STRENGTHENED (strictly above entry), not merely held.
            adverse_drop = first_fill - entry_price
            if adverse_drop > settings.TOPUP_MAX_ADVERSE_DROP and current_conviction <= entry_conviction:
                logger.info(
                    f"LIVE TOP-UP SKIPPED — averaging down without conviction: "
                    f"{market.market_id} {signal.direction.upper()} ask "
                    f"{entry_price:.2%} is {adverse_drop:.2%} below first fill "
                    f"{first_fill:.2%}; conviction {current_conviction:.0%} not "
                    f"above entry {entry_conviction:.0%}"
                )
                return None

            contracts = gap
            logger.info(
                f"LIVE TOP-UP: {market.market_id} {signal.direction.upper()} — "
                f"working {working}/{target_contracts} (filled {filled}, resting "
                f"{resting}, undet {undetermined}), ordering {contracts} more"
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

        # Include the ceiled Kalshi fee so the balance check reserves the full
        # cash the order will actually consume (contracts × price + fee), not
        # just the notional — otherwise a just-barely-affordable order can be
        # rejected by Kalshi for insufficient funds.
        order_cost = round(
            contracts * entry_price
            + kalshi_trade_fee_ceil(entry_price, contracts, settings.KALSHI_FEE_RATE),
            2,
        )
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

        if signal.direction == "no":
            result = await client.place_order(
                ticker=market.market_id, side="no",
                count=contracts, no_price=limit_price_cents,
            )
        else:
            result = await client.place_order(
                ticker=market.market_id, side="yes",
                count=contracts, yes_price=limit_price_cents,
            )

        order = result.get("order", result)
        order_id = order.get("id") or order.get("order_id")
        # Kalshi reports the fill price on the side traded. For a NO order use
        # no_price; fall back to converting yes_price. Default to entry on miss.
        if signal.direction == "no":
            fill_price_raw = order.get("no_price")
            if fill_price_raw is None and order.get("yes_price") is not None:
                fill_price_raw = 100 - order["yes_price"]
        else:
            fill_price_raw = order.get("yes_price")
            if fill_price_raw is None and order.get("no_price") is not None:
                fill_price_raw = 100 - order["no_price"]
        if fill_price_raw is None:
            fill_price_raw = order.get("fill_price")
        fill_price = (fill_price_raw / 100.0) if fill_price_raw else entry_price

        new_order = {"id": order_id, "price": fill_price, "n": contracts}

        if anchor is not None:
            # ── Top-up: fold into the existing position row ───────────────────
            # Blend cost basis by total cost / total contracts. Exact for P&L
            # (basis × count reproduces total cost); settlement later recomputes
            # the basis from each order's ACTUAL fills.
            prior_cost = (anchor.contracts or 0) * (anchor.entry_price or 0.0)
            add_cost   = contracts * fill_price
            new_total  = (anchor.contracts or 0) + contracts

            anchor.contracts   = new_total
            anchor.entry_price = round((prior_cost + add_cost) / new_total, 4) if new_total else fill_price
            anchor.fill_price  = anchor.entry_price
            anchor.kelly_size  = round((anchor.kelly_size or 0.0) + order_cost, 2)
            anchor.orders      = json.dumps(_position_orders(anchor) + [new_order])
            # Track the high-water Kelly target so fill % is measured against the
            # largest size this position ever pursued.
            anchor.target_contracts = max(anchor.target_contracts or 0, target_contracts)
            # Refresh the signal snapshot to the scan that justified the add.
            anchor.model_prob   = signal.model_probability
            anchor.market_price = signal.market_probability
            anchor.edge         = signal.edge
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
            target_contracts = target_contracts,   # Kelly target to fill; fill % measures against this
            entry_price      = entry_price,
            fill_price       = fill_price,
            kalshi_order_id  = order_id,
            orders           = json.dumps([new_order]),
            forecast_mean    = signal.ensemble_mean,
            forecast_std     = signal.ensemble_std,
            model_data_age_hours = getattr(signal, "model_data_age_hours", None),
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
    from weatherbot.data.weather import et_today
    today = et_today()   # ET, not UTC — markets resolve on the local ET day
    settled = []

    db = SessionLocal()
    try:
        pending = db.query(Trade).filter(
            Trade.is_paper == False,
            Trade.resolved == False,
            # Strictly before today (ET): a market resolving *today* doesn't
            # settle until 11:59 PM ET, so it isn't eligible yet.
            Trade.resolution_date < today.isoformat(),
        ).all()

        if not pending:
            logger.debug("No live trades ready for settlement")
            return []

        logger.info(f"Settling {len(pending)} live trade(s)...")

        from weatherbot.data.kalshi_client import KalshiClient
        client = KalshiClient()

        # Hours after resolution_date before we force-expire resting orders.
        # Kalshi cancels all resting orders at market close, but the API may
        # lag. After this window we treat resting as expired to prevent a trade
        # from blocking settlement indefinitely.
        RESTING_EXPIRY_HOURS = 6

        for trade in pending:
            # ── Step 1: tally ACTUAL fills across every order in the position ──
            total_filled, total_resting, undetermined, per_order = \
                await _position_fill_status(client, trade)

            if total_resting > 0:
                # Check if the resolution date is far enough past that the
                # resting order must have expired (Kalshi clears the book at
                # market close). If so, treat resting as 0 and settle on fills.
                try:
                    res_date = date.fromisoformat(trade.resolution_date)
                    from zoneinfo import ZoneInfo
                    et = ZoneInfo("America/New_York")
                    # Markets resolve at end of ET calendar day (11:59 PM ET)
                    resolution_et = datetime(
                        res_date.year, res_date.month, res_date.day,
                        23, 59, 0, tzinfo=et,
                    )
                    hours_past = (
                        datetime.now(timezone.utc) - resolution_et.astimezone(timezone.utc)
                    ).total_seconds() / 3600.0
                    if hours_past < RESTING_EXPIRY_HOURS:
                        logger.info(
                            f"Skipping {trade.ticker} — {total_resting} contract(s) still "
                            f"resting, {hours_past:.1f}h past resolution (expiry threshold "
                            f"{RESTING_EXPIRY_HOURS}h)"
                        )
                        continue
                    logger.warning(
                        f"{trade.ticker}: {total_resting} resting contract(s) after "
                        f"{hours_past:.1f}h — treating as expired, settling on "
                        f"{total_filled} confirmed fill(s)"
                    )
                    # Fall through: settle on total_filled only (resting ignored)
                except Exception as _e:
                    logger.warning(f"Resting expiry check failed for {trade.ticker}: {_e}")
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

            if filled_count == 0 and undetermined > 0:
                # We have undetermined orders and zero confirmed fills.
                # Booking P&L on unverifiable fills risks a phantom loss.
                # Skip and retry next settlement cycle.
                logger.warning(
                    f"Skipping {trade.ticker}: {undetermined} undetermined contract(s) "
                    f"with 0 confirmed fills — deferring to next cycle"
                )
                continue

            if filled_count == 0:
                logger.warning(
                    f"{trade.ticker}: no resolvable fills but status not all-cancelled "
                    f"— settling cancelled with zero P&L"
                )
                trade.resolved    = True
                trade.result      = "cancelled"
                trade.pnl         = 0.0
                trade.resolved_at = datetime.utcnow()
                settled.append(trade)
                continue
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

            pnl = settlement_pnl(trade.entry_price, filled_count, we_win,
                                 settings.KALSHI_FEE_RATE)
            result = "win" if we_win else "loss"

            trade.resolved     = True
            trade.result       = result
            trade.pnl          = round(pnl, 2)
            trade.yes_resolved = bool(yes_wins)   # was: actual_temp = 0/1 proxy
            trade.resolved_at  = datetime.utcnow()
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
        # The batch rolled back — nothing persisted, so surface nothing.
        # Settling (and notifying) retries cleanly next cycle.
        return []
    finally:
        db.close()

    return settled


async def _fetch_order_fill(client, order_id, fallback_n, label="") -> Optional[Tuple[int, int]]:
    """
    Resolve a single Kalshi order to (filled_contracts, resting_contracts), or
    None if the status couldn't be determined (transient API error, unknown
    status, or a MISSING order id). `fallback_n` is only used when a 'filled'
    order reports no count. `label` names the trade for the missing-id warning.
    """
    if not order_id:
        # No order id recorded — UNDETERMINED, not "assume fully filled": if
        # placement succeeded but the id failed to parse, booking it as filled
        # would create phantom P&L. Return None so _position_fill_status and
        # settlement defer conservatively.
        logger.warning(
            f"Order fill undetermined for {label or 'trade'}: no order id "
            f"recorded ({fallback_n} contract(s)) — not assuming filled"
        )
        return None
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
        res = await _fetch_order_fill(client, o.get("id"), n, label=trade.ticker)
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
