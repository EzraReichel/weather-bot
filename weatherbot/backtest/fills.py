"""
Execution / fill modeling for the backtest.

taker:  cross the spread now — fill at the recorded ask on the bet side, pay the
        taker fee. Matches what the live bot does today.

maker:  post a passive limit (at bid / mid / bid+offset on the bet side) and fill
        only if the captured price path later trades through it before resolution
        — i.e. our side's ask falls to <= our post price. Pay the (configurable)
        maker fee. Unfilled maker orders open no position.

Bid-coverage caveat: Kalshi's bulk listing often omits yes_bid (see
weather_markets.WeatherMarket), so bt_market_snapshots.yes_bid is frequently 0.
When bid is missing the maker post price falls back to the ask (no spread
capture) and the fill is flagged bid_thin=True so the UI can show how much of a
maker backtest is actually resting on real bid data.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Optional
from zoneinfo import ZoneInfo

from weatherbot.core.strategy import StrategyParams

ET = ZoneInfo("America/New_York")


@dataclass
class Fill:
    filled: bool
    price: float          # fill price on the bet side (0-1)
    fee_coef: float       # fee coefficient applied at settlement
    bid_thin: bool = False
    note: str = ""


def _side_ask(point, bet_side: str) -> Optional[float]:
    a = point.yes_ask if bet_side == "yes" else point.no_ask
    return a if a and a > 0 else None


def _post_price(bet_side: str, yes_ask: float, yes_bid: float, no_ask: float, params: StrategyParams):
    """Return (post_price, bid_thin) for a maker order on the bet side."""
    if bet_side == "yes":
        ask, bid = yes_ask, yes_bid
    else:
        ask = no_ask
        # no_bid isn't captured; approximate from the yes side: no_bid ≈ 1 - yes_ask.
        bid = (1.0 - yes_ask) if yes_ask and yes_ask > 0 else 0.0
    bid_thin = not (bid and bid > 0)
    if params.maker_post_at == "bid" and not bid_thin:
        post = bid
    elif params.maker_post_at == "bid_plus" and not bid_thin:
        post = min(ask, bid + params.maker_offset)
    elif params.maker_post_at == "mid" and not bid_thin:
        post = round((ask + bid) / 2.0, 4)
    else:
        post = ask  # no usable bid → can't improve; behaves like taker
    return post, bid_thin


def resolve_fill(
    bet_side: str,
    point,                       # the _MarketPoint at as_of (entry snapshot)
    path: List,                  # full _MarketPoint list for this ticker (asc)
    params: StrategyParams,
    as_of: datetime,
    target_date,
) -> Optional[Fill]:
    """Decide whether/at what price an order on ``bet_side`` fills."""
    ask_now = _side_ask(point, bet_side)
    if ask_now is None:
        return None

    if params.fill_mode != "maker":
        return Fill(filled=True, price=ask_now, fee_coef=params.taker_fee_coef)

    # Maker: post passively, see if the path trades through before resolution.
    post, bid_thin = _post_price(bet_side, point.yes_ask, point.yes_bid, point.no_ask, params)
    if post >= ask_now:
        # Can't post inside the spread (no bid data) — treat as immediate fill at
        # the ask, but charge the maker fee and flag it as thin.
        return Fill(filled=True, price=ask_now, fee_coef=params.maker_fee_coef,
                    bid_thin=True, note="no-bid: posted at ask")

    resolution_end = datetime(target_date.year, target_date.month, target_date.day,
                              23, 59, 0, tzinfo=ET).astimezone(timezone.utc).replace(tzinfo=None)
    naive_as_of = (as_of.astimezone(timezone.utc).replace(tzinfo=None)
                   if as_of.tzinfo is not None else as_of)
    for p in path:
        if p.captured_at < naive_as_of or p.captured_at > resolution_end:
            continue
        a = _side_ask(p, bet_side)
        if a is not None and a <= post:
            return Fill(filled=True, price=post, fee_coef=params.maker_fee_coef, bid_thin=bid_thin)
    return Fill(filled=False, price=post, fee_coef=params.maker_fee_coef, bid_thin=bid_thin,
                note="maker order never filled")
