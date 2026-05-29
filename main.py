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

from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
import uvicorn

from weatherbot.config import settings
from weatherbot.models.weather_db import init_db

# ── FastAPI app ───────────────────────────────────────────────────────────────
app = FastAPI(docs_url=None, redoc_url=None)

FRONTEND_DIR = Path(__file__).parent / "dashboard"


@app.get("/health")
async def health():
    return JSONResponse({"status": "ok"})


@app.post("/report")
@app.get("/report")
async def trigger_report():
    """Post a full paper trading report to Discord. Hit from anywhere."""
    from weatherbot.notifications.discord import send_paper_report
    ok = send_paper_report()
    return JSONResponse({"sent": ok})


@app.get("/app.jsx")
async def serve_jsx():
    """Serve the JSX component with correct MIME type for Babel standalone."""
    return FileResponse(FRONTEND_DIR / "app.jsx", media_type="application/javascript")


@app.get("/", include_in_schema=False)
async def serve_ui():
    return FileResponse(FRONTEND_DIR / "index.html")


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

    if live_only:
        return f"SELECT {fields}, is_paper FROM trades WHERE NOT is_paper ORDER BY {order} LIMIT {limit}"

    if paper_only:
        live_paper_q = f"SELECT {fields}, is_paper FROM trades WHERE is_paper"
        if has_paper:
            paper_q = f"SELECT {fields}, TRUE AS is_paper FROM paper_trades"
            return f"SELECT * FROM ({paper_q} UNION ALL {live_paper_q}) t ORDER BY {order} LIMIT {limit}"
        return f"{live_paper_q} ORDER BY {order} LIMIT {limit}"

    # default: all trades
    trades_q = f"SELECT {fields}, is_paper FROM trades"
    if has_paper:
        paper_q = f"SELECT {fields}, TRUE AS is_paper FROM paper_trades"
        return f"SELECT * FROM ({paper_q} UNION ALL {trades_q}) t ORDER BY {order} LIMIT {limit}"
    return f"{trades_q} ORDER BY {order} LIMIT {limit}"


def _rows_to_trades(rows) -> list:
    cols = [
        "id", "ticker", "city", "metric", "threshold_f", "side", "market_direction",
        "model_prob", "market_price", "edge", "contracts", "entry_price", "kelly_size",
        "created_at", "resolution_date", "resolved", "result", "pnl", "actual_temp",
        "resolved_at", "is_paper",
    ]
    out = []
    for row in rows:
        d = dict(zip(cols, row))
        d["created_at"] = d["created_at"].isoformat() if d["created_at"] else None
        d["resolved_at"] = d["resolved_at"].isoformat() if d["resolved_at"] else None
        d["resolved"] = bool(d["resolved"])
        d["is_paper"] = bool(d["is_paper"])
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
    from weatherbot.data.kalshi_client import fetch_live_balance
    from weatherbot.models.weather_db import SessionLocal, engine
    from sqlalchemy import text, inspect as sa_inspect

    live_balance = await fetch_live_balance()

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
            if t["resolved"] and t["pnl"] is not None and t["resolved_at"]:
                cumulative += t["pnl"]
                points.append({
                    "t": t["resolved_at"],
                    "bankroll": round(cumulative, 2),
                    "pnl": round(t["pnl"], 2),
                    "result": t["result"],
                    "ticker": t["ticker"],
                })
        return {"points": points, "current": round(live_balance, 2), "initial": initial}
    finally:
        db.close()



@app.get("/api/config")
async def api_config():
    return {
        "LIVE_TRADING": settings.LIVE_TRADING,
        "INITIAL_BANKROLL": settings.INITIAL_BANKROLL,
        "KELLY_FRACTION": settings.KELLY_FRACTION,
        "SCAN_INTERVAL_SECONDS": settings.SCAN_INTERVAL_SECONDS,
        "MIN_EDGE_THRESHOLD": settings.MIN_EDGE_THRESHOLD,
        "KALSHI_FEE_RATE": settings.KALSHI_FEE_RATE,
        "WEATHER_MAX_TRADE_SIZE": settings.WEATHER_MAX_TRADE_SIZE,
        "LIVE_MAX_TRADE_SIZE": settings.LIVE_MAX_TRADE_SIZE,
        "TRADING_HOURS_START": settings.TRADING_HOURS_START,
        "TRADING_HOURS_END": settings.TRADING_HOURS_END,
        "MIN_ASK_SIZE": settings.MIN_ASK_SIZE,
        "MIN_VOLUME_24H": settings.MIN_VOLUME_24H,
        "WEATHER_MIN_ENTRY_PRICE": settings.WEATHER_MIN_ENTRY_PRICE,
        "WEATHER_MAX_ENTRY_PRICE": settings.WEATHER_MAX_ENTRY_PRICE,
        "CITY_OVERRIDE": settings.CITY_OVERRIDE,
    }


@app.post("/api/config")
async def api_config_update(request: Request):
    body = await request.json()
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


# ── Startup / shutdown hooks ─────────────────────────────────────────────────
@app.on_event("startup")
async def on_startup():
    logger.info("=" * 60)
    logger.info("Kalshi Weather Arb Bot")
    logger.info("=" * 60)

    init_db()

    if not settings.LIVE_TRADING:
        logger.info("=" * 60)
        logger.info("PAPER TRADING MODE — no real trades will be placed")
        logger.info("=" * 60)
    logger.info(f"Min edge: {settings.MIN_EDGE_THRESHOLD:.0%}  |  "
                f"Kelly: {settings.KELLY_FRACTION:.0%}  |  "
                f"Fee rate: {settings.KALSHI_FEE_RATE:.0%}  |  "
                f"Scan: {settings.SCAN_INTERVAL_SECONDS}s")

    try:
        from weatherbot.data.kalshi_client import fetch_live_balance
        from weatherbot.notifications.discord import send_startup_message
        startup_balance = asyncio.run(fetch_live_balance())
        send_startup_message(not settings.LIVE_TRADING, startup_balance)
    except Exception as e:
        logger.warning(f"Discord startup ping failed: {e}")

    from weatherbot.models.trade import init_trade_db
    init_trade_db()
    logger.info("Trade DB initialized")

    from weatherbot.core.scheduler import start_scheduler
    start_scheduler()
    logger.info(f"Mission Control UI at http://0.0.0.0:{settings.PORT}")


@app.on_event("shutdown")
async def on_shutdown():
    from weatherbot.core.scheduler import stop_scheduler
    stop_scheduler()
    logger.info("Shutdown complete.")


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=settings.PORT,
        log_level="warning",   # uvicorn access logs off; our logger handles it
        reload=False,
    )
