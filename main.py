#!/usr/bin/env python3
"""
Kalshi Weather Arb Bot — single-process worker.

Starts the async scan loop and a lightweight health-check HTTP server.
Designed to run as a Railway worker.
"""
import asyncio
import logging
import os
import signal
import sys
from datetime import datetime
from http.server import BaseHTTPRequestHandler, HTTPServer
from threading import Thread

# ── Logging setup ────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("weatherbot")

# ── Imports ──────────────────────────────────────────────────────────────────
from backend.config import settings
from backend.models.database import init_db, SessionLocal, BotState


# ── Health check HTTP server (Railway keeps-alive) ───────────────────────────
class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(b'{"status":"ok"}')

    def log_message(self, format, *args):
        pass  # Suppress HTTP access logs


def start_health_server(port: int):
    server = HTTPServer(("0.0.0.0", port), HealthHandler)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    logger.info(f"Health check listening on :{port}")


# ── Main async loop ───────────────────────────────────────────────────────────
async def main():
    logger.info("=" * 60)
    logger.info("Kalshi Weather Arb Bot")
    logger.info("=" * 60)

    # Health check
    start_health_server(settings.PORT)

    # Init DB and bot state
    logger.info("Initializing database...")
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
            logger.info(f"New bot state created — bankroll ${settings.INITIAL_BANKROLL:,.2f}")
        else:
            state.is_running = True
            db.commit()
            logger.info(
                f"Loaded bot state — bankroll ${state.bankroll:,.2f}  "
                f"P&L ${state.total_pnl:+,.2f}  trades {state.total_trades}"
            )
    finally:
        db.close()

    logger.info(f"Simulation mode: {settings.SIMULATION_MODE}")
    logger.info(f"Min edge threshold: {settings.MIN_EDGE_THRESHOLD:.0%}")
    logger.info(f"Kelly fraction: {settings.KELLY_FRACTION:.0%}")
    logger.info(f"Kalshi fee rate: {settings.KALSHI_FEE_RATE:.0%}")
    logger.info(f"Scan interval: {settings.SCAN_INTERVAL_SECONDS}s")
    logger.info(f"Cities: {settings.WEATHER_CITIES}")

    # Discord startup notification
    try:
        from backend.notifications.discord import send_startup_message
        send_startup_message(settings.SIMULATION_MODE, settings.INITIAL_BANKROLL)
    except Exception as e:
        logger.warning(f"Failed to send startup Discord message: {e}")

    # Start scheduler (handles scan, settlement, daily summary)
    from backend.core.scheduler import start_scheduler
    start_scheduler()

    logger.info("Bot is running. Press Ctrl+C to stop.")

    # Keep alive — handle SIGTERM from Railway gracefully
    stop_event = asyncio.Event()

    def _shutdown(sig, frame):
        logger.info(f"Received signal {sig}, shutting down...")
        stop_event.set()

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    await stop_event.wait()

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


if __name__ == "__main__":
    asyncio.run(main())
