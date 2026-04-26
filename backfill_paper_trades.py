#!/usr/bin/env python3
"""
Backfill paper trades from historical signals in local SQLite weatherbot.db
into the Render Postgres DB, then settle any that have Kalshi results.

Run once: python backfill_paper_trades.py
"""
import asyncio
import json
import logging
import re
import sqlite3
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s: %(message)s")
logger = logging.getLogger("backfill")

# ── Parse ticker into components ──────────────────────────────────────────────
SERIES_TO_CITY = {
    "KXHIGHNY":    ("nyc",           "high"),
    "KXHIGHNY0":   ("nyc",           "high"),
    "KXHIGHCHI":   ("chicago",       "high"),
    "KXHIGHMIA":   ("miami",         "high"),
    "KXHIGHLAX":   ("los_angeles",   "high"),
    "KXHIGHDEN":   ("denver",        "high"),
    "KXLOWTCHI":   ("chicago",       "low"),
    "KXLOWTMIA":   ("miami",         "low"),
    "KXLOWTDEN":   ("denver",        "low"),
    "KXLOWTNY":    ("nyc",           "low"),
    "KXLOWTLAX":   ("los_angeles",   "low"),
}

def parse_ticker(ticker: str):
    """Return (city, metric, threshold_f, resolution_date_str, market_direction)."""
    # e.g. KXHIGHNY-26APR08-T53  or  KXHIGHLAX-26APR08-B75.5
    m = re.match(r"^([A-Z0-9]+)-(\d{2}[A-Z]{3}\d{2})-([TB])([\d.]+)$", ticker)
    if not m:
        return None
    series, date_str, side_letter, threshold_str = m.groups()
    city, metric = SERIES_TO_CITY.get(series, (series.lower(), "high"))
    threshold_f = float(threshold_str)

    # Parse date: 26APR08 → 2026-04-08
    month_map = {"JAN":"01","FEB":"02","MAR":"03","APR":"04","MAY":"05","JUN":"06",
                 "JUL":"07","AUG":"08","SEP":"09","OCT":"10","NOV":"11","DEC":"12"}
    yr = "20" + date_str[:2]
    mo = month_map.get(date_str[2:5], "01")
    dy = date_str[5:7]
    resolution_date = f"{yr}-{mo}-{dy}"

    # T = threshold (below), B = binary (above)
    market_direction = "below" if side_letter == "T" else "above"
    return city, metric, threshold_f, resolution_date, market_direction


# ── Fetch Kalshi result ───────────────────────────────────────────────────────
async def fetch_kalshi_result(ticker: str):
    """Return 'yes', 'no', or None if not resolved."""
    from backend.data.kalshi_client import KalshiClient, kalshi_credentials_present
    if not kalshi_credentials_present():
        logger.warning("No Kalshi credentials — cannot fetch results")
        return None
    try:
        client = KalshiClient()
        data = await client.get_market(ticker)
        market = data.get("market", data)
        result = market.get("result")
        return result if result in ("yes", "no") else None
    except Exception as e:
        logger.debug(f"Kalshi lookup failed for {ticker}: {e}")
        return None


# ── Main backfill ─────────────────────────────────────────────────────────────
async def main():
    from backend.config import settings
    from backend.models.database import engine, init_db
    from backend.models.paper_trade import init_paper_db, PaperTrade, PaperSessionLocal

    logger.info("Initializing Render Postgres DB...")
    init_db()
    init_paper_db()

    # Load signals from local SQLite
    sqlite_path = Path(__file__).parent / "weatherbot.db"
    if not sqlite_path.exists():
        logger.error("weatherbot.db not found")
        return

    con = sqlite3.connect(sqlite_path)
    con.row_factory = sqlite3.Row
    cur = con.cursor()
    cur.execute("""
        SELECT id, market_ticker, direction, model_probability, market_price,
               edge, confidence, suggested_size, timestamp
        FROM signals
        WHERE edge >= 0.08
        ORDER BY timestamp ASC
    """)
    rows = cur.fetchall()
    con.close()
    logger.info(f"Found {len(rows)} signals with edge >= 8% in local weatherbot.db")

    db = PaperSessionLocal()
    inserted = 0
    skipped = 0

    try:
        for row in rows:
            ticker = row["market_ticker"]
            parsed = parse_ticker(ticker)
            if not parsed:
                logger.warning(f"Could not parse ticker: {ticker}")
                skipped += 1
                continue

            city, metric, threshold_f, resolution_date, market_direction = parsed

            # Dedup: skip if already in Postgres
            existing = db.query(PaperTrade).filter(PaperTrade.ticker == ticker).first()
            if existing:
                skipped += 1
                continue

            direction = row["direction"]  # "yes" or "no"
            entry_price = row["market_price"]
            if entry_price <= 0:
                entry_price = 0.50  # default if missing
            kelly_size = row["suggested_size"] or 100.0
            contracts = max(1, int(kelly_size / entry_price))
            created_at = datetime.fromisoformat(row["timestamp"])

            pt = PaperTrade(
                ticker           = ticker,
                city             = city,
                metric           = metric,
                threshold_f      = threshold_f,
                side             = direction,
                market_direction = market_direction,
                agreement        = "MEDIUM",
                model_probs      = "{}",
                model_prob       = row["model_probability"],
                market_price     = entry_price,
                edge             = row["edge"],
                confidence       = row["confidence"],
                kelly_size       = kelly_size,
                contracts        = contracts,
                entry_price      = entry_price,
                forecast_mean    = None,
                forecast_std     = None,
                created_at       = created_at,
                resolution_date  = resolution_date,
                resolved         = False,
            )
            db.add(pt)
            inserted += 1

        db.commit()
        logger.info(f"Inserted {inserted} paper trades, skipped {skipped}")

    except Exception as e:
        db.rollback()
        logger.error(f"Insert failed: {e}", exc_info=True)
        return
    finally:
        db.close()

    # Now settle all unresolved trades
    logger.info("Checking Kalshi for resolved markets...")
    from backend.config import settings as cfg

    db = PaperSessionLocal()
    try:
        pending = db.query(PaperTrade).filter(PaperTrade.resolved == False).all()
        logger.info(f"Checking {len(pending)} unresolved paper trades...")

        settled_count = wins = losses = 0
        total_pnl = 0.0

        for pt in pending:
            kalshi_result = await fetch_kalshi_result(pt.ticker)
            if kalshi_result is None:
                logger.info(f"  {pt.ticker}: not resolved yet (market may still be open or expired without result)")
                continue

            yes_wins = (kalshi_result == "yes")
            we_win = yes_wins if pt.side == "yes" else not yes_wins

            if we_win:
                pnl = (1.0 - pt.entry_price) * pt.contracts * (1.0 - cfg.KALSHI_FEE_RATE)
                result = "win"
                wins += 1
            else:
                pnl = pt.entry_price * pt.contracts * -1.0
                result = "loss"
                losses += 1

            pt.resolved    = True
            pt.result      = result
            pt.pnl         = round(pnl, 2)
            pt.resolved_at = datetime.utcnow()
            pt.actual_temp = 1.0 if yes_wins else 0.0
            total_pnl     += pnl
            settled_count += 1

            icon = "✅" if result == "win" else "❌"
            logger.info(f"  {icon} {pt.ticker}: Kalshi={kalshi_result.upper()} side={pt.side.upper()} P&L=${pnl:+.2f}")

        db.commit()
        logger.info(f"\nSettled {settled_count} trades: {wins}W / {losses}L  Total P&L: ${total_pnl:+.2f}")

    except Exception as e:
        db.rollback()
        logger.error(f"Settlement failed: {e}", exc_info=True)
    finally:
        db.close()

    # Print final report
    db = PaperSessionLocal()
    try:
        from sqlalchemy import func
        from sqlalchemy.orm import Session

        all_trades = db.query(PaperTrade).all()
        resolved   = [t for t in all_trades if t.resolved]
        unresolved = [t for t in all_trades if not t.resolved]
        wins_list  = [t for t in resolved if t.result == "win"]
        losses_list= [t for t in resolved if t.result == "loss"]
        total_pnl  = sum(t.pnl for t in resolved if t.pnl is not None)

        print("\n" + "═"*60)
        print("  BACKFILL COMPLETE — PAPER TRADING REPORT")
        print("═"*60)
        print(f"  Total trades backfilled:  {len(all_trades)}")
        print(f"  Resolved:                 {len(resolved)}")
        print(f"  Still pending:            {len(unresolved)}")
        if resolved:
            print(f"  W/L Record:               {len(wins_list)}W / {len(losses_list)}L")
            pct = len(wins_list) / len(resolved) * 100
            print(f"  Win rate:                 {pct:.0f}%")
            print(f"  Total P&L:                ${total_pnl:+.2f}")
            avg_edge = sum(t.edge for t in all_trades) / len(all_trades) if all_trades else 0
            print(f"  Avg edge at entry:        {avg_edge:+.1%}")
        print("═"*60)

        if unresolved:
            print(f"\n  STILL PENDING ({len(unresolved)}):")
            for t in unresolved:
                print(f"    {t.ticker}  {t.side.upper()}  resolution={t.resolution_date}")
    finally:
        db.close()


if __name__ == "__main__":
    asyncio.run(main())
