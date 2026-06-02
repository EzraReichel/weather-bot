#!/usr/bin/env python3
"""
scripts/backtest_db_status.py — create the bt_* tables and report archive status.

Run this once to provision the backtest tables on the Render DB, and any time
after to see how much data has accumulated.

Usage:
    source .venv/bin/activate
    python scripts/backtest_db_status.py
"""
import sys
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from sqlalchemy import text

from weatherbot.config import settings
from weatherbot.models.backtest_db import init_backtest_db
from weatherbot.models.weather_db import SessionLocal


def _scalar(db, sql, **params):
    try:
        return db.execute(text(sql), params).scalar()
    except Exception as e:
        return f"<err: {e}>"


def main() -> None:
    print("=" * 64)
    print("  Backtest archive status")
    db_url = settings.DATABASE_URL
    masked = db_url.split("@")[-1] if "@" in db_url else db_url
    print(f"  DB: ...@{masked}")
    print("=" * 64)

    print("\nEnsuring bt_* tables exist...")
    init_backtest_db()
    print("  OK")

    db = SessionLocal()
    try:
        print("\nRow counts:")
        for tbl in ("bt_scans", "bt_market_snapshots", "bt_weather_snapshots",
                    "bt_signal_snapshots", "bt_settlements"):
            n = _scalar(db, f"SELECT COUNT(*) FROM {tbl}")
            print(f"  {tbl:24s} {n}")

        print("\nCapture window:")
        first = _scalar(db, "SELECT MIN(started_at) FROM bt_scans")
        last = _scalar(db, "SELECT MAX(started_at) FROM bt_scans")
        print(f"  first scan: {first}")
        print(f"  last  scan: {last}")

        print("\nDistinct coverage:")
        cities = _scalar(db, "SELECT COUNT(DISTINCT city_key) FROM bt_market_snapshots")
        tickers = _scalar(db, "SELECT COUNT(DISTINCT ticker) FROM bt_market_snapshots")
        settled = _scalar(db, "SELECT COUNT(*) FROM bt_settlements")
        print(f"  cities seen:      {cities}")
        print(f"  distinct tickers: {tickers}")
        print(f"  settled tickers:  {settled}")

        # Backtest-ready = settled tickers that also have >=1 signal snapshot
        ready = _scalar(db, """
            SELECT COUNT(DISTINCT s.ticker)
            FROM bt_settlements s
            JOIN bt_signal_snapshots g ON g.ticker = s.ticker
        """)
        print(f"  backtest-ready:   {ready}  (settled + has signal snapshot)")
    finally:
        db.close()
    print("\nDone.")


if __name__ == "__main__":
    main()
