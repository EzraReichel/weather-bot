#!/usr/bin/env python3
"""
Kalshi Weather Arb Bot — single-process worker.

Serves the Mission Control UI at / and runs the scan loop in the background.
Railway health check hits GET /health → 200.
"""
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
from fastapi import FastAPI
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
import uvicorn

from backend.config import settings
from backend.models.database import init_db, SessionLocal, BotState

# ── FastAPI app ───────────────────────────────────────────────────────────────
app = FastAPI(docs_url=None, redoc_url=None)

FRONTEND_DIR = Path(__file__).parent / "frontend"


@app.get("/health")
async def health():
    return JSONResponse({"status": "ok"})


@app.get("/app.jsx")
async def serve_jsx():
    """Serve the JSX component with correct MIME type for Babel standalone."""
    return FileResponse(FRONTEND_DIR / "app.jsx", media_type="application/javascript")


@app.get("/", include_in_schema=False)
async def serve_ui():
    return FileResponse(FRONTEND_DIR / "index.html")


# ── Startup / shutdown hooks ─────────────────────────────────────────────────
@app.on_event("startup")
async def on_startup():
    logger.info("=" * 60)
    logger.info("Kalshi Weather Arb Bot")
    logger.info("=" * 60)

    init_db()

    db = SessionLocal()
    try:
        state = db.query(BotState).first()
        if not state:
            state = BotState(
                bankroll=settings.INITIAL_BANKROLL,
                total_trades=0,
                winning_trades=0,
                total_pnl=0.0,
                is_running=True,
            )
            db.add(state)
            db.commit()
            logger.info(f"New bot state — bankroll ${settings.INITIAL_BANKROLL:,.2f}")
        else:
            state.is_running = True
            db.commit()
            logger.info(
                f"Loaded state — bankroll ${state.bankroll:,.2f}  "
                f"P&L ${state.total_pnl:+,.2f}  trades {state.total_trades}"
            )
    finally:
        db.close()

    logger.info(f"Simulation mode: {settings.SIMULATION_MODE}")
    logger.info(f"Min edge: {settings.MIN_EDGE_THRESHOLD:.0%}  |  "
                f"Kelly: {settings.KELLY_FRACTION:.0%}  |  "
                f"Fee rate: {settings.KALSHI_FEE_RATE:.0%}  |  "
                f"Scan: {settings.SCAN_INTERVAL_SECONDS}s")

    try:
        from backend.notifications.discord import send_startup_message
        send_startup_message(settings.SIMULATION_MODE, settings.INITIAL_BANKROLL)
    except Exception as e:
        logger.warning(f"Discord startup ping failed: {e}")

    from backend.core.scheduler import start_scheduler
    start_scheduler()
    logger.info(f"Mission Control UI at http://0.0.0.0:{settings.PORT}")


@app.on_event("shutdown")
async def on_shutdown():
    from backend.core.scheduler import stop_scheduler
    stop_scheduler()

    db = SessionLocal()
    try:
        state = db.query(BotState).first()
        if state:
            state.is_running = False
            db.commit()
    finally:
        db.close()
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
