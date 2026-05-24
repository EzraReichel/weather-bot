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
from fastapi import FastAPI
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
        from weatherbot.notifications.discord import send_startup_message
        send_startup_message(not settings.LIVE_TRADING, settings.INITIAL_BANKROLL)
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
