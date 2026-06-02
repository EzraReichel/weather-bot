"""
Example drop-in strategy: "conviction".

Demonstrates the intended pattern for new algorithms — reuse the shared decide()
core (so the probability model + filter cascade stay identical and drift-free),
and only override the *bet acceptance* rule on top of it. Here: only take trades
where the cross-model agreement is HIGH or MEDIUM and the absolute edge clears a
stricter bar; everything else is turned into a no-bet (edge zeroed).

Copy this file to add your own algorithm: implement
``fn(day, params, bankroll) -> Decision`` and call ``register("name", fn)``.
"""
from __future__ import annotations

from typing import Optional

from weatherbot.backtest.decision import Decision, decide as base_decide
from weatherbot.backtest.registry import register
from weatherbot.backtest.world import WeatherDay
from weatherbot.core.strategy import StrategyParams

CONVICTION_MIN_EDGE = 0.12


def decide_conviction(day: WeatherDay, params: StrategyParams, bankroll: Optional[float] = None) -> Optional[Decision]:
    dec = base_decide(day, params, bankroll)
    if dec is None:
        return None
    # Only bet on agreeing models with a clear edge; otherwise stand down.
    if dec.agreement == "LOW" or abs(dec.edge) < CONVICTION_MIN_EDGE:
        dec.edge = 0.0
        dec.suggested_size = 0.0
        if not dec.filter_reason:
            dec.filter_reason = "low_conviction"
    return dec


register("conviction", decide_conviction)
