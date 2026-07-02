#!/usr/bin/env python3
"""
Kalshi Weather Arb Bot — single-process worker.

Serves the Mission Control UI at / and runs the scan loop in the background.
Railway health check hits GET /health → 200.
"""
# Load .env BEFORE any backend imports so env vars are present when
# pydantic-settings instantiates the Settings object.
from dotenv import load_dotenv
load_dotenv()

import asyncio
import logging
import os
import signal
import sys
from contextlib import asynccontextmanager
from pathlib import Path

# ── Logging setup ────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("weatherbot")

# ── Imports ──────────────────────────────────────────────────────────────────
import json

from fastapi import BackgroundTasks, FastAPI, Request
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
import uvicorn

from weatherbot.config import settings
from weatherbot.models.weather_db import init_db

# ── Lifespan (startup / shutdown) ─────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("=" * 60)
    logger.info("Kalshi Weather Arb Bot")
    logger.info("=" * 60)

    init_db()

    if not settings.LIVE_TRADING:
        logger.info("=" * 60)
        logger.info("PAPER TRADING MODE — no real trades will be placed")
        logger.info("=" * 60)

    # Dashboard auth posture (see the dashboard_auth middleware).
    if not settings.DASHBOARD_TOKEN:
        if settings.LIVE_TRADING:
            logger.warning("!" * 60)
            logger.warning("SECURITY: DASHBOARD_TOKEN is UNSET while LIVE_TRADING=true.")
            logger.warning("Dashboard is FAIL-CLOSED — every route except /health "
                           "returns 503 until DASHBOARD_TOKEN is set.")
            logger.warning("!" * 60)
        else:
            logger.info("DASHBOARD_TOKEN unset (LIVE_TRADING=false) — dashboard open for local dev")
    else:
        logger.info("Dashboard auth enabled (Bearer token / ?token= / cookie)")
    logger.info(f"Min edge: {settings.MIN_EDGE_THRESHOLD:.0%}  |  "
                f"Kelly: {settings.KELLY_FRACTION:.0%}  |  "
                f"Fee rate: {settings.KALSHI_FEE_RATE:.0%}  |  "
                f"Scan: {settings.SCAN_INTERVAL_SECONDS}s")

    try:
        from weatherbot.data.kalshi_client import fetch_live_balance
        from weatherbot.notifications.discord import send_startup_message
        startup_balance = await fetch_live_balance()
        send_startup_message(not settings.LIVE_TRADING, startup_balance)
    except Exception as e:
        logger.warning(f"Discord startup ping failed: {e}")

    from weatherbot.models.trade import init_trade_db
    init_trade_db()
    logger.info("Trade DB initialized")

    if settings.BACKTEST_CAPTURE:
        try:
            from weatherbot.models.backtest_db import init_backtest_db
            init_backtest_db()
            logger.info("Backtest capture DB initialized")
        except Exception as e:
            logger.warning(f"Backtest DB init failed (capture will retry lazily): {e}")

    from weatherbot.core.scheduler import start_scheduler
    start_scheduler()
    logger.info(f"Mission Control UI at http://0.0.0.0:{settings.PORT}")

    yield

    # ── Shutdown ──────────────────────────────────────────────────────────────
    from weatherbot.core.scheduler import stop_scheduler
    stop_scheduler()
    logger.info("Shutdown complete.")


# ── FastAPI app ───────────────────────────────────────────────────────────────
app = FastAPI(docs_url=None, redoc_url=None, lifespan=lifespan)

FRONTEND_DIR = Path(__file__).parent / "dashboard"

# ── Dashboard authentication ─────────────────────────────────────────────────
# Every route except GET/HEAD /health requires the dashboard token, supplied as
# an `Authorization: Bearer <token>` header, a one-time `?token=` query param
# (which we then persist in an HttpOnly cookie), or that cookie on later requests.
# The bot trades real money, so the admin panel must never be open under live
# trading: if DASHBOARD_TOKEN is unset while LIVE_TRADING=true we FAIL CLOSED and
# serve 503 on everything but /health. With LIVE_TRADING=false an unset token
# leaves the panel open for local-dev convenience.
import secrets as _secrets

_DASH_COOKIE = "wb_dash_token"


def _is_open_path(request: Request) -> bool:
    """Only the health check is reachable without auth."""
    return request.url.path == "/health" and request.method in ("GET", "HEAD")


@app.middleware("http")
async def dashboard_auth(request: Request, call_next):
    if _is_open_path(request):
        return await call_next(request)

    token = settings.DASHBOARD_TOKEN
    if not token:
        # No token configured. Fail closed under live trading; open otherwise.
        if settings.LIVE_TRADING:
            return JSONResponse(
                {"detail": "Dashboard locked: DASHBOARD_TOKEN is unset while "
                           "LIVE_TRADING=true. Set DASHBOARD_TOKEN to unlock."},
                status_code=503,
            )
        return await call_next(request)

    # Token configured — accept it from the Bearer header, the ?token= param, or
    # the cookie. Constant-time compare to avoid leaking it via timing.
    provided = None
    auth = request.headers.get("Authorization", "")
    if auth.startswith("Bearer "):
        provided = auth[len("Bearer "):].strip()
    query_token = request.query_params.get("token")
    if provided is None and query_token is not None:
        provided = query_token
    if provided is None:
        provided = request.cookies.get(_DASH_COOKIE)

    if not provided or not _secrets.compare_digest(provided, token):
        return JSONResponse({"detail": "Unauthorized"}, status_code=401)

    response = await call_next(request)
    # First entry via ?token= — persist it in an HttpOnly cookie so the browser
    # carries it on subsequent asset/API requests without exposing it to JS.
    if query_token is not None and _secrets.compare_digest(query_token, token):
        response.set_cookie(
            _DASH_COOKIE, token, httponly=True, samesite="lax",
            max_age=60 * 60 * 24 * 30,
        )
    return response


@app.api_route("/health", methods=["GET", "HEAD"])
async def health():
    return JSONResponse({"status": "ok"})


@app.post("/report")
@app.get("/report")
async def trigger_report():
    """Post a full paper trading report to Discord. Hit from anywhere."""
    from weatherbot.notifications.discord import send_paper_report
    from weatherbot.data.kalshi_client import fetch_live_balance
    bankroll = await fetch_live_balance()
    ok = send_paper_report(bankroll=bankroll)
    return JSONResponse({"sent": ok})


# The dashboard is transpiled in-browser by Babel from /app.jsx. Without an
# explicit no-cache header, browsers heuristically cache the file, so after a
# deploy the page keeps running the old UI (e.g. missing the Portfolio value
# card) until a hard refresh. Force revalidation so a redeploy always shows.
_NO_CACHE = {"Cache-Control": "no-cache, must-revalidate"}


@app.get("/app.jsx")
async def serve_jsx():
    """Serve the JSX component with correct MIME type for Babel standalone."""
    return FileResponse(FRONTEND_DIR / "app.jsx", media_type="application/javascript", headers=_NO_CACHE)


@app.get("/", include_in_schema=False)
async def serve_ui():
    return FileResponse(FRONTEND_DIR / "index.html", headers=_NO_CACHE)


@app.get("/backtest.jsx")
async def serve_backtest_jsx():
    """Serve the Backtest Lab JSX with the right MIME for Babel standalone."""
    return FileResponse(FRONTEND_DIR / "backtest.jsx", media_type="application/javascript", headers=_NO_CACHE)


@app.get("/backtest", include_in_schema=False)
async def serve_backtest_ui():
    """Standalone Backtest Lab page (kept separate from the main dashboard)."""
    return FileResponse(FRONTEND_DIR / "backtest.html", headers=_NO_CACHE)


CITIES_JSON = Path(__file__).parent / "weatherbot" / "cities.json"
ENV_FILE = Path(__file__).parent / ".env"


def _all_trades_sql(has_paper: bool, order: str = "created_at DESC", limit: int = 2000, live_only: bool = False, paper_only: bool = False) -> str:
    """Build a query across paper_trades (legacy) and trades (current).

    live_only  → only is_paper=false rows from trades
    paper_only → legacy paper_trades + is_paper=true rows from trades
    default    → everything (both)
    """
    fields = (
        "id, ticker, city, metric, threshold_f, side, market_direction, "
        "model_prob, market_price, edge, contracts, entry_price, kelly_size, "
        "created_at, resolution_date, resolved, result, pnl, actual_temp, resolved_at"
    )
    # The current `trades` table has target_contracts/orders/fill_price; the
    # legacy `paper_trades` table does not — select NULL there so the UNION
    # columns line up. `orders` is the JSON per-leg breakdown (original + top-ups).
    trades_sel = f"{fields}, target_contracts, orders, fill_price"
    paper_sel  = f"{fields}, NULL AS target_contracts, NULL AS orders, NULL AS fill_price"

    if live_only:
        return f"SELECT {trades_sel}, is_paper FROM trades WHERE NOT is_paper ORDER BY {order} LIMIT {limit}"

    if paper_only:
        live_paper_q = f"SELECT {trades_sel}, is_paper FROM trades WHERE is_paper"
        if has_paper:
            paper_q = f"SELECT {paper_sel}, TRUE AS is_paper FROM paper_trades"
            return f"SELECT * FROM ({paper_q} UNION ALL {live_paper_q}) t ORDER BY {order} LIMIT {limit}"
        return f"{live_paper_q} ORDER BY {order} LIMIT {limit}"

    # default: all trades
    trades_q = f"SELECT {trades_sel}, is_paper FROM trades"
    if has_paper:
        paper_q = f"SELECT {paper_sel}, TRUE AS is_paper FROM paper_trades"
        return f"SELECT * FROM ({paper_q} UNION ALL {trades_q}) t ORDER BY {order} LIMIT {limit}"
    return f"{trades_q} ORDER BY {order} LIMIT {limit}"


def _rows_to_trades(rows) -> list:
    cols = [
        "id", "ticker", "city", "metric", "threshold_f", "side", "market_direction",
        "model_prob", "market_price", "edge", "contracts", "entry_price", "kelly_size",
        "created_at", "resolution_date", "resolved", "result", "pnl", "actual_temp",
        "resolved_at", "target_contracts", "orders", "fill_price", "is_paper",
    ]
    out = []
    for row in rows:
        d = dict(zip(cols, row))
        d["created_at"] = d["created_at"].isoformat() if d["created_at"] else None
        d["resolved_at"] = d["resolved_at"].isoformat() if d["resolved_at"] else None
        d["resolved"] = bool(d["resolved"])
        d["is_paper"] = bool(d["is_paper"])
        # Per-leg breakdown: [{"id","price","n"}, ...]. Parse to a list so the UI
        # can show each order's fill price and contract count for topped-up rows.
        try:
            d["orders"] = json.loads(d["orders"]) if d.get("orders") else []
        except (ValueError, TypeError):
            d["orders"] = []
        d["ticker"] = d["ticker"] or ""
        d["city"] = d["city"] or ""
        d["metric"] = d["metric"] or ""
        d["side"] = d["side"] or ""
        d["market_direction"] = d["market_direction"] or ""
        d["result"] = d["result"] or ""
        d["resolution_date"] = d["resolution_date"] or ""
        out.append(d)
    return out


@app.get("/api/trades")
async def api_trades(limit: int = 2000):
    """Live trades only (is_paper=false)."""
    from weatherbot.models.weather_db import SessionLocal, engine
    from sqlalchemy import text, inspect as sa_inspect
    db = SessionLocal()
    try:
        has_paper = "paper_trades" in sa_inspect(engine).get_table_names()
        sql = _all_trades_sql(has_paper, order="created_at DESC", limit=limit, live_only=True)
        rows = db.execute(text(sql)).fetchall()
        trades = _rows_to_trades(rows)
        # Cancelled orders never filled (zero P&L, no real position) — hide them
        # so the dashboard reflects only trades that actually transacted. Common
        # cause of dupes: a runaway re-buy that places several orders for one
        # position, most of which cancel unfilled.
        trades = [t for t in trades if t.get("result") != "cancelled"]
        return {"trades": trades, "total": len(trades)}
    finally:
        db.close()


@app.get("/api/paper-trades")
async def api_paper_trades(limit: int = 5000):
    """Paper trades only (legacy paper_trades table + is_paper=true rows)."""
    from weatherbot.models.weather_db import SessionLocal, engine
    from sqlalchemy import text, inspect as sa_inspect
    db = SessionLocal()
    try:
        has_paper = "paper_trades" in sa_inspect(engine).get_table_names()
        sql = _all_trades_sql(has_paper, order="created_at DESC", limit=limit, paper_only=True)
        rows = db.execute(text(sql)).fetchall()
        trades = _rows_to_trades(rows)
        return {"trades": trades, "total": len(trades)}
    finally:
        db.close()


@app.get("/api/bankroll")
async def api_bankroll():
    """Bankroll — current value from live Kalshi account, history from settled trades."""
    from weatherbot.data.kalshi_client import fetch_balance_breakdown
    from weatherbot.models.weather_db import SessionLocal, engine
    from sqlalchemy import text, inspect as sa_inspect

    # Full account composition; `current` (the trading bankroll) follows the
    # KELLY_USE_EQUITY basis so the chart/header match how sizing actually works.
    balance = await fetch_balance_breakdown()
    live_balance = balance["equity"] if settings.KELLY_USE_EQUITY else balance["cash"]

    db = SessionLocal()
    try:
        has_paper = "paper_trades" in sa_inspect(engine).get_table_names()
        sql = _all_trades_sql(has_paper, order="resolved_at ASC", limit=10000, live_only=True)
        all_rows = db.execute(text(sql)).fetchall()
        all_trades = _rows_to_trades(all_rows)

        initial = settings.INITIAL_BANKROLL
        cumulative = initial
        points = [{"t": None, "bankroll": initial}]
        for t in all_trades:
            if t["result"] == "cancelled":
                continue  # never filled — no bankroll impact, skip the flat point
            if t["resolved"] and t["pnl"] is not None and t["resolved_at"]:
                cumulative += t["pnl"]
                points.append({
                    "t": t["resolved_at"],
                    "bankroll": round(cumulative, 2),
                    "pnl": round(t["pnl"], 2),
                    "result": t["result"],
                    "ticker": t["ticker"],
                })
        return {
            "points": points,
            "current": round(live_balance, 2),
            "initial": initial,
            "cash": round(balance["cash"], 2),
            "positions": round(balance["positions"], 2),
            "equity": round(balance["equity"], 2),
        }
    finally:
        db.close()


@app.get("/api/equity-history")
async def api_equity_history(range: str = "week"):
    """Portfolio value (cash + open positions) over time, from equity snapshots.

    `range` selects the window: day=24h, week=7d, month=30d. Series is
    downsampled to a sane point count so wide windows stay light.
    """
    from datetime import datetime, timedelta
    from weatherbot.models.weather_db import SessionLocal, EquitySnapshot

    days = {"day": 1, "week": 7, "month": 30}.get(range, 7)
    cutoff = datetime.utcnow() - timedelta(days=days)
    MAX_POINTS = 400

    db = SessionLocal()
    try:
        rows = (
            db.query(EquitySnapshot)
            .filter(EquitySnapshot.timestamp >= cutoff)
            .order_by(EquitySnapshot.timestamp)
            .all()
        )
        # Downsample by striding, but always keep the most recent point.
        if len(rows) > MAX_POINTS:
            stride = len(rows) // MAX_POINTS + 1
            rows = rows[::stride] + ([rows[-1]] if (len(rows) - 1) % stride else [])
        points = [
            {
                "t": r.timestamp.isoformat(),
                "equity": round(r.equity or 0, 2),
                "cash": round(r.cash or 0, 2),
                "positions": round(r.positions or 0, 2),
            }
            for r in rows
        ]
        return {"points": points, "range": range}
    finally:
        db.close()


@app.get("/api/config")
async def api_config():
    return {
        "LIVE_TRADING": settings.LIVE_TRADING,
        "INITIAL_BANKROLL": settings.INITIAL_BANKROLL,
        "KELLY_FRACTION": settings.KELLY_FRACTION,
        "KELLY_USE_EQUITY": settings.KELLY_USE_EQUITY,
        "SCAN_INTERVAL_SECONDS": settings.SCAN_INTERVAL_SECONDS,
        "MIN_EDGE_THRESHOLD": settings.MIN_EDGE_THRESHOLD,
        "KALSHI_FEE_RATE": settings.KALSHI_FEE_RATE,
        "WEATHER_MAX_TRADE_SIZE": settings.WEATHER_MAX_TRADE_SIZE,
        "LIVE_MAX_TRADE_SIZE": settings.LIVE_MAX_TRADE_SIZE,
        "MIN_ASK_SIZE": settings.MIN_ASK_SIZE,
        "MIN_VOLUME_24H": settings.MIN_VOLUME_24H,
        "WEATHER_MIN_ENTRY_PRICE": settings.WEATHER_MIN_ENTRY_PRICE,
        "WEATHER_MAX_ENTRY_PRICE": settings.WEATHER_MAX_ENTRY_PRICE,
        "CITY_OVERRIDE": settings.CITY_OVERRIDE,
    }


# Only these tunables may be written back to .env from the dashboard. Anything
# outside this set is rejected — in particular LIVE_TRADING, KALSHI_*,
# DATABASE_URL, DISCORD_*, and DASHBOARD_TOKEN must NEVER be settable here, and
# keys that render.yaml overrides in prod would be silently ineffective anyway.
# The auth middleware already gates this route; the allowlist is defense in depth.
CONFIG_WRITE_ALLOWLIST = frozenset({
    "MIN_EDGE_THRESHOLD", "KELLY_FRACTION", "SCAN_INTERVAL_SECONDS",
    "WEATHER_MAX_TRADE_SIZE", "LIVE_MAX_TRADE_SIZE", "MIN_ASK_SIZE",
    "MIN_VOLUME_24H", "CITY_OVERRIDE",
})


@app.post("/api/config")
async def api_config_update(request: Request):
    body = await request.json()

    # Reject the whole request if any key is outside the allowlist — name the
    # first offender so the caller knows what was refused. Nothing is written
    # unless every key is permitted.
    for key in body:
        if key not in CONFIG_WRITE_ALLOWLIST:
            return JSONResponse(
                {"error": f"key not writable via /api/config: {key}",
                 "allowed": sorted(CONFIG_WRITE_ALLOWLIST)},
                status_code=400,
            )

    if not ENV_FILE.exists():
        lines = []
    else:
        lines = ENV_FILE.read_text().splitlines()

    for key, value in body.items():
        if isinstance(value, bool):
            str_val = "true" if value else "false"
        else:
            str_val = str(value)
        found = False
        for i, line in enumerate(lines):
            # Match both active and commented-out keys
            stripped = line.lstrip("# ").strip()
            if stripped.startswith(f"{key}="):
                lines[i] = f"{key}={str_val}"
                found = True
                break
        if not found:
            lines.append(f"{key}={str_val}")

    ENV_FILE.write_text("\n".join(lines) + "\n")
    return {"updated": list(body.keys()), "restart_required": True}


@app.get("/api/cities")
async def api_cities():
    return json.loads(CITIES_JSON.read_text())


@app.post("/api/cities/{city_key}/toggle")
async def api_city_toggle(city_key: str):
    data = json.loads(CITIES_JSON.read_text())
    if city_key not in data:
        return JSONResponse({"error": "City not found"}, status_code=404)
    data[city_key]["enabled"] = not data[city_key].get("enabled", True)
    CITIES_JSON.write_text(json.dumps(data, indent=2) + "\n")
    return {"city": city_key, "enabled": data[city_key]["enabled"]}


@app.get("/api/git-commits")
async def api_git_commits():
    import subprocess
    try:
        result = subprocess.run(
            ["git", "log", "--format=%H|%ai|%s", "--no-merges"],
            capture_output=True, text=True,
            cwd=str(Path(__file__).parent),
            timeout=5,
        )
        commits = []
        for line in result.stdout.strip().split("\n"):
            if not line or "|" not in line:
                continue
            parts = line.split("|", 2)
            if len(parts) == 3:
                h, date, msg = parts
                commits.append({"hash": h[:8], "date": date[:10], "message": msg.strip()})
        return {"commits": commits}
    except Exception as e:
        return {"commits": [], "error": str(e)}


# ── Backtest Lab API ──────────────────────────────────────────────────────────
# All read the bt_* archive / result tables; runs never touch the live trade
# tables. The engine package (weatherbot.backtest) is imported lazily here so the
# live worker's import graph stays clean.

@app.get("/api/backtest/strategies")
async def api_bt_strategies():
    from weatherbot.backtest.registry import list_strategies
    return {"strategies": list_strategies()}


@app.get("/api/backtest/params/defaults")
async def api_bt_param_defaults():
    """Seed the param editor with the strategy that reproduces live behavior."""
    from weatherbot.core.strategy import StrategyParams
    return {"params": StrategyParams.live_default().to_dict()}


@app.get("/api/backtest/runs")
async def api_bt_runs(limit: int = 50):
    from weatherbot.models.weather_db import SessionLocal
    from weatherbot.models.backtest_db import BtRun, init_backtest_db
    init_backtest_db()
    db = SessionLocal()
    try:
        rows = db.query(BtRun).order_by(BtRun.created_at.desc()).limit(limit).all()
        return {"runs": [{
            "run_id": r.run_id, "name": r.name, "strategy": r.strategy,
            "status": r.status, "error": r.error,
            "start": r.start_date, "end": r.end_date, "cities": r.cities or [],
            "n_trades": r.n_trades, "total_pnl": r.total_pnl,
            "initial_bankroll": r.initial_bankroll, "final_bankroll": r.final_bankroll,
            "created_at": r.created_at.isoformat() if r.created_at else None,
            "metrics": r.metrics,
        } for r in rows]}
    finally:
        db.close()


@app.get("/api/backtest/runs/{run_id}")
async def api_bt_run(run_id: str, ledger: bool = True):
    from weatherbot.models.weather_db import SessionLocal
    from weatherbot.models.backtest_db import BtRun, BtRunTrade, init_backtest_db
    init_backtest_db()
    db = SessionLocal()
    try:
        r = db.query(BtRun).filter(BtRun.run_id == run_id).first()
        if r is None:
            return JSONResponse({"error": "run not found"}, status_code=404)
        out = {
            "run_id": r.run_id, "name": r.name, "strategy": r.strategy,
            "status": r.status, "error": r.error, "params": r.params,
            "start": r.start_date, "end": r.end_date, "cities": r.cities or [],
            "metrics": r.metrics, "equity_curve": r.equity_curve,
            "n_trades": r.n_trades, "total_pnl": r.total_pnl,
            "initial_bankroll": r.initial_bankroll, "final_bankroll": r.final_bankroll,
            "created_at": r.created_at.isoformat() if r.created_at else None,
        }
        if ledger:
            trades = db.query(BtRunTrade).filter(BtRunTrade.run_id == run_id).all()
            out["ledger"] = [{
                "ticker": t.ticker, "city_key": t.city_key, "metric": t.metric,
                "direction": t.direction, "threshold_f": t.threshold_f,
                "target_date": t.target_date, "bet_side": t.bet_side,
                "model_prob": t.model_prob, "market_prob": t.market_prob, "edge": t.edge,
                "agreement": t.agreement, "fill_mode": t.fill_mode, "filled": t.filled,
                "entry_price": t.entry_price, "contracts": t.contracts, "size": t.size,
                "resolved": t.resolved, "result": t.result, "settlement": t.settlement,
                "pnl": t.pnl,
            } for t in trades]
        return out
    finally:
        db.close()


def _run_backtest_job(run_id: str, body: dict):
    """Executed in a background thread — fills in the queued BtRun row."""
    from weatherbot.backtest.runner import run_backtest
    from weatherbot.core.strategy import StrategyParams
    from weatherbot.models.weather_db import SessionLocal
    from weatherbot.models.backtest_db import BtRun
    try:
        merged = {**StrategyParams.live_default().to_dict(), **(body.get("params") or {})}
        params = StrategyParams.from_dict(merged)
        run_backtest(
            params=params,
            start=body["start"], end=body["end"],
            cities=body.get("cities") or None,
            strategy=body.get("strategy", "default"),
            name=body.get("name"),
            run_id=run_id, persist=True,
        )
    except Exception as e:
        logger.error(f"Backtest run {run_id} failed: {e}", exc_info=True)
        db = SessionLocal()
        try:
            r = db.query(BtRun).filter(BtRun.run_id == run_id).first()
            if r is not None:
                r.status = "error"
                r.error = str(e)
                db.commit()
        finally:
            db.close()


@app.post("/api/backtest/run")
async def api_bt_run_start(request: Request, background_tasks: BackgroundTasks):
    """Queue a backtest. Returns a run_id immediately; poll /api/backtest/runs/{id}."""
    import uuid as _uuid
    from weatherbot.models.weather_db import SessionLocal
    from weatherbot.models.backtest_db import BtRun, init_backtest_db
    body = await request.json()
    if not body.get("start") or not body.get("end"):
        return JSONResponse({"error": "start and end (YYYY-MM-DD) are required"}, status_code=400)

    init_backtest_db()
    run_id = _uuid.uuid4().hex
    db = SessionLocal()
    try:
        db.add(BtRun(
            run_id=run_id, name=body.get("name"), strategy=body.get("strategy", "default"),
            params=body.get("params") or {}, start_date=body["start"], end_date=body["end"],
            cities=body.get("cities") or [], status="queued",
        ))
        db.commit()
    finally:
        db.close()

    background_tasks.add_task(_run_backtest_job, run_id, body)
    return {"run_id": run_id, "status": "queued"}


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=settings.PORT,
        log_level="warning",   # uvicorn access logs off; our logger handles it
        reload=False,
    )
