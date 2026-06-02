"""
run_backtest — orchestrates a full run: load archive → simulate → metrics →
persist to bt_runs / bt_run_trades. Returns a result dict for the CLI / API.
"""
from __future__ import annotations

import logging
import uuid
from datetime import datetime
from typing import List, Optional

from weatherbot.backtest.archive import load_archive
from weatherbot.backtest.metrics import compute_metrics
from weatherbot.backtest.portfolio import simulate
from weatherbot.backtest.registry import get_strategy
from weatherbot.core.strategy import StrategyParams
from weatherbot.models.backtest_db import BtRun, BtRunTrade, init_backtest_db
from weatherbot.models.weather_db import SessionLocal

logger = logging.getLogger("weatherbot")


def run_backtest(
    params: StrategyParams,
    start: str,
    end: str,
    cities: Optional[List[str]] = None,
    strategy: str = "default",
    name: Optional[str] = None,
    persist: bool = True,
    run_id: Optional[str] = None,
) -> dict:
    """
    Replay ``strategy`` over [start, end] with ``params``. ``run_id`` may be
    supplied so an API can create a queued BtRun row first and have this fill it.
    """
    init_backtest_db()
    run_id = run_id or uuid.uuid4().hex
    strategy_fn = get_strategy(strategy)

    db = SessionLocal()
    try:
        data = load_archive(db, start=start, end=end, cities=cities)
        sim = simulate(data, params, start, end, strategy_fn=strategy_fn)
        metrics = compute_metrics(sim["ledger"], sim["equity_curve"], params.initial_bankroll)

        if persist:
            _persist(db, run_id, params, start, end, cities, strategy, name, sim, metrics)

        logger.info(
            f"[bt] run {run_id[:8]} '{strategy}': {metrics['n_filled']} fills, "
            f"P&L ${metrics['total_pnl']:+.2f}, final ${metrics['final_bankroll']:.2f}, "
            f"hit {metrics['hit_rate']}"
        )
        return {
            "run_id": run_id, "strategy": strategy, "name": name,
            "start": start, "end": end, "cities": cities or [],
            "params": params.to_dict(), "metrics": metrics,
            "equity_curve": sim["equity_curve"], "ledger": sim["ledger"],
        }
    finally:
        db.close()


def _persist(db, run_id, params, start, end, cities, strategy, name, sim, metrics):
    # Upsert the run row (an API may have pre-created a queued row).
    run = db.query(BtRun).filter(BtRun.run_id == run_id).first()
    if run is None:
        run = BtRun(run_id=run_id, created_at=datetime.utcnow())
        db.add(run)
    run.name = name
    run.strategy = strategy
    run.params = params.to_dict()
    run.start_date = start
    run.end_date = end
    run.cities = cities or []
    run.status = "done"
    run.error = None
    run.metrics = metrics
    run.equity_curve = sim["equity_curve"]
    run.n_trades = metrics["n_filled"]
    run.initial_bankroll = metrics["initial_bankroll"]
    run.final_bankroll = metrics["final_bankroll"]
    run.total_pnl = metrics["total_pnl"]

    # Replace any prior ledger rows for this run, then insert fresh.
    db.query(BtRunTrade).filter(BtRunTrade.run_id == run_id).delete()
    for t in sim["ledger"]:
        db.add(BtRunTrade(
            run_id=run_id, ticker=t["ticker"], city_key=t["city_key"], metric=t["metric"],
            direction=t["direction"], threshold_f=t["threshold_f"], target_date=t["target_date"],
            as_of=t["as_of"], bet_side=t["bet_side"], model_prob=t["model_prob"],
            market_prob=t["market_prob"], edge=t["edge"], agreement=t["agreement"],
            fill_mode=t["fill_mode"], filled=t["filled"], entry_price=t["entry_price"],
            contracts=t["contracts"], size=t["size"], resolved=t["resolved"],
            result=t["result"], settlement=t["settlement"], pnl=t["pnl"],
        ))
    db.commit()
