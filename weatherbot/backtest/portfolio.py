"""
Day-by-day portfolio simulator.

Walks the calendar clock across the sim window. On each sim day, at each model-run
ET time, it rebuilds a WeatherDay for every still-open market and calls the
strategy's ``decide()``. Actionable decisions that clear the dedup/affordability
gates open positions (via the fill model); positions settle from bt_settlements
once their resolution date has passed. Mirrors the live portfolio rules: one
position per (city, date, metric) correlated group, Kelly sized against available
bankroll (cash or equity basis).
"""
from __future__ import annotations

import logging
from datetime import date, datetime, timedelta, timezone
from typing import Callable, Dict, List, Optional
from zoneinfo import ZoneInfo

from weatherbot.backtest.archive import BacktestData
from weatherbot.backtest.decision import Decision, decide as default_decide
from weatherbot.backtest.fills import resolve_fill
from weatherbot.core.strategy import StrategyParams

logger = logging.getLogger("weatherbot")
ET = ZoneInfo("America/New_York")

StrategyFn = Callable[[object, StrategyParams, float], Optional[Decision]]


def _side_mark(point, bet_side: str) -> float:
    if point is None:
        return 0.0
    if bet_side == "yes":
        return point.yes_ask or 0.0
    return point.no_ask or 0.0


def simulate(
    data: BacktestData,
    params: StrategyParams,
    start: str,
    end: str,
    strategy_fn: Optional[StrategyFn] = None,
) -> Dict:
    """Run the simulation. Returns {equity_curve, ledger}."""
    strategy_fn = strategy_fn or default_decide

    cash = float(params.initial_bankroll)
    open_pos: Dict[str, dict] = {}          # ticker -> position
    entered_groups: set = set()             # (city, date, metric) correlation guard
    ledger: List[dict] = []
    equity_curve: List[dict] = [{"t": None, "cash": round(cash, 2), "equity": round(cash, 2), "n_open": 0}]

    start_d = date.fromisoformat(start)
    end_d = date.fromisoformat(end)
    run_times = params.model_run_hours_et

    def _settle_due(as_of_date: date):
        nonlocal cash
        for ticker in list(open_pos.keys()):
            pos = open_pos[ticker]
            if date.fromisoformat(pos["target_date"]) >= as_of_date:
                continue  # not resolved yet
            sett = data.settlements.get(ticker)
            if not sett:
                continue  # ground truth not available — leave open (mark unsettled at end)
            yes_wins = sett["result"] == "yes"
            we_win = yes_wins if pos["bet_side"] == "yes" else not yes_wins
            entry = pos["entry_price"]; n = pos["contracts"]
            fee = pos["fee_coef"] * n * entry * (1.0 - entry)
            payout_per = 1.0 if we_win else 0.0
            cash += n * payout_per - fee
            pnl = (payout_per - entry) * n - fee
            pos["resolved"] = True
            pos["result"] = "win" if we_win else "loss"
            pos["settlement"] = sett["result"]
            pos["pnl"] = round(pnl, 2)
            del open_pos[ticker]

    def _equity(as_of: datetime) -> float:
        mark = 0.0
        for ticker, pos in open_pos.items():
            mp = data.market_at(ticker, as_of)
            mark += pos["contracts"] * _side_mark(mp, pos["bet_side"])
        return cash + mark

    day = start_d
    while day <= end_d:
        _settle_due(day)

        for (h, m) in run_times:
            as_of = datetime(day.year, day.month, day.day, h, m, tzinfo=ET)
            available = cash if params.bankroll_basis == "cash" else _equity(as_of)
            if available <= 0:
                continue

            for ticker, meta in data.tickers.items():
                if ticker in open_pos:
                    continue
                if meta.target_date < day:           # already resolved
                    continue
                group = (meta.city_key, meta.target_date.isoformat(), meta.metric)
                if group in entered_groups:
                    continue

                wd = data.build_day(ticker, as_of)
                if wd is None:
                    continue
                dec = strategy_fn(wd, params, available)
                if dec is None or not dec.passes_threshold:
                    continue

                entry_ask = dec.market.yes_price if dec.direction == "yes" else dec.market.no_price
                if entry_ask <= 0 or dec.suggested_size < entry_ask:
                    continue  # undersized / no price

                fill = resolve_fill(dec.direction, data.market_at(ticker, as_of),
                                    data.market_paths.get(ticker, []), params, as_of, meta.target_date)
                if fill is None:
                    continue

                row = {
                    "ticker": ticker, "city_key": meta.city_key, "metric": meta.metric,
                    "direction": meta.direction, "threshold_f": meta.threshold_f,
                    "target_date": meta.target_date.isoformat(), "as_of": as_of.astimezone(timezone.utc).replace(tzinfo=None),
                    "bet_side": dec.direction, "model_prob": dec.model_probability,
                    "market_prob": dec.market_probability, "edge": dec.edge, "agreement": dec.agreement,
                    "fill_mode": params.fill_mode, "bid_thin": fill.bid_thin,
                    "filled": fill.filled, "entry_price": round(fill.price, 4),
                    "contracts": 0, "size": 0.0, "fee_coef": fill.fee_coef,
                    "resolved": False, "result": None, "settlement": None, "pnl": None,
                }

                if not fill.filled:
                    ledger.append(row)
                    continue

                price = fill.price
                contracts = max(1, int(dec.suggested_size / price))
                contracts = min(contracts, int(available / price))
                if contracts < 1:
                    continue  # can't afford
                cost = round(contracts * price, 2)
                cash -= cost
                row["contracts"] = contracts
                row["size"] = cost
                open_pos[ticker] = row
                entered_groups.add(group)
                ledger.append(row)
                available = cash if params.bankroll_basis == "cash" else _equity(as_of)

        eod = datetime(day.year, day.month, day.day, 23, 59, tzinfo=ET)
        equity_curve.append({
            "t": day.isoformat(),
            "cash": round(cash, 2),
            "equity": round(_equity(eod), 2),
            "n_open": len(open_pos),
        })
        day += timedelta(days=1)

    # Mark any still-open (unsettled) positions for reporting.
    for pos in open_pos.values():
        pos["result"] = "unsettled"

    return {"equity_curve": equity_curve, "ledger": ledger}
