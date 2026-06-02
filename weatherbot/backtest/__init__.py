"""
Backtest strategy engine.

Replays the bt_* forward archive through a shared, pure ``decide()`` core under
a chosen ``StrategyParams`` and simulates a portfolio day-by-day, producing an
equity curve, a trade ledger, and summary metrics.

Isolation contract: this package is imported ONLY by the backtest CLI / API
endpoints — never by the live worker (main.py / scheduler.py). It reads the
bt_* archive read-only and writes results to bt_runs / bt_run_trades. It never
touches the live `trades` / `paper_trades` tables.
"""
