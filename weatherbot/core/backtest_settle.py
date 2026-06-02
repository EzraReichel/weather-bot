"""
Settlement backfill for the backtest archive.

Fills bt_settlements with authoritative outcomes (result + observed temperature)
from Kalshi's historical market API, for every captured ticker whose target_date
has passed. These are the labels backtests score against.

Importable from both the scheduler (daily job) and scripts/backfill_settlements.py.
"""
import asyncio
import logging
from datetime import date, datetime, timedelta
from typing import Optional

from sqlalchemy import text

from weatherbot.data.kalshi_client import KalshiClient, kalshi_credentials_present
from weatherbot.models.backtest_db import BtSettlement, init_backtest_db
from weatherbot.models.weather_db import SessionLocal

logger = logging.getLogger("weatherbot")

RATE_LIMIT_SLEEP = 0.2   # seconds between Kalshi requests


async def fetch_settlement(client: KalshiClient, ticker: str) -> Optional[dict]:
    """Return {result, expiration_value, settled_ts} for a settled ticker, or None."""
    try:
        data = await client.get(f"/historical/markets/{ticker}")
    except Exception:
        try:
            data = await client.get(f"/markets/{ticker}")
        except Exception:
            return None

    mkt = data.get("market") or data
    result = mkt.get("result")
    if result not in ("yes", "no"):
        return None

    exp_val = mkt.get("expiration_value")
    try:
        exp_f = float(exp_val) if exp_val not in (None, "") else None
    except (TypeError, ValueError):
        exp_f = None

    settled_ts = None
    for k in ("settled_time", "close_time", "expiration_time"):
        raw = mkt.get(k)
        if raw:
            try:
                settled_ts = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
                break
            except Exception:
                pass

    return {"result": result, "expiration_value": exp_f, "settled_ts": settled_ts}


def _pending_tickers(db, days_back: int, limit: int):
    """Distinct tradeable tickers with a passed target_date and no settlement yet."""
    cutoff = (date.today() - timedelta(days=days_back)).isoformat()
    today = date.today().isoformat()
    lim = f"LIMIT {limit}" if limit and limit > 0 else ""
    sql = text(f"""
        SELECT m.ticker, MIN(m.city_key), MIN(m.metric), MIN(m.direction),
               MIN(m.threshold_f), MIN(m.target_date)
        FROM bt_market_snapshots m
        LEFT JOIN bt_settlements s ON s.ticker = m.ticker
        WHERE m.tradeable = TRUE
          AND m.target_date < :today
          AND m.target_date >= :cutoff
          AND s.ticker IS NULL
        GROUP BY m.ticker
        ORDER BY MIN(m.target_date) DESC
        {lim}
    """)
    return db.execute(sql, {"today": today, "cutoff": cutoff}).fetchall()


async def backfill_settlements(days_back: int = 30, limit: int = 0, verbose: bool = False) -> dict:
    """
    Backfill outcomes for settled tickers. Returns {found, filled, missing}.
    Best-effort; logs and continues on per-ticker errors.
    """
    if not kalshi_credentials_present():
        logger.warning("[bt] settlement backfill skipped — Kalshi credentials missing")
        return {"found": 0, "filled": 0, "missing": 0}

    init_backtest_db()
    client = KalshiClient()
    db = SessionLocal()
    filled = missing = 0
    try:
        rows = _pending_tickers(db, days_back, limit)
        if verbose:
            print(f"Found {len(rows)} unsettled tickers (target_date in last {days_back}d).")

        for ticker, city_key, metric, direction, threshold_f, target_date in rows:
            await asyncio.sleep(RATE_LIMIT_SLEEP)
            res = await fetch_settlement(client, ticker)
            if res is None:
                missing += 1
                continue
            db.add(BtSettlement(
                ticker=ticker, city_key=city_key, metric=metric, direction=direction,
                threshold_f=threshold_f, target_date=target_date,
                result=res["result"], expiration_value=res["expiration_value"],
                settled_ts=res["settled_ts"],
            ))
            filled += 1
            if filled % 50 == 0:
                db.commit()
                if verbose:
                    print(f"  ...{filled} settled so far")

        db.commit()
        logger.info(f"[bt] settlement backfill: {filled} filled, {missing} not-yet-settled")
        return {"found": len(rows), "filled": filled, "missing": missing}
    except Exception as e:
        logger.warning(f"[bt] settlement backfill error: {e}", exc_info=True)
        try:
            db.rollback()
        except Exception:
            pass
        return {"found": 0, "filled": filled, "missing": missing}
    finally:
        db.close()
