#!/usr/bin/env python3
"""
End-to-end audit of the paper trading pipeline.
Run: python audit_paper_trades.py
"""
from dotenv import load_dotenv
load_dotenv()

import asyncio
import json
import logging
import sys
from datetime import date, datetime, timedelta

logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")
logger = logging.getLogger("audit")


def section(title: str):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print('='*60)


# ── 1. Config ─────────────────────────────────────────────────────────────────
section("1. CONFIGURATION")
from backend.config import settings
print(f"  DATABASE_URL:      {settings.DATABASE_URL[:40]}...")
print(f"  DRY_RUN:           {settings.DRY_RUN}")
print(f"  MIN_EDGE:          {settings.MIN_EDGE_THRESHOLD:.0%}")
print(f"  INITIAL_BANKROLL:  ${settings.INITIAL_BANKROLL:,.0f}")
print(f"  KELLY_FRACTION:    {settings.KELLY_FRACTION:.0%}")
print(f"  KALSHI_FEE_RATE:   {settings.KALSHI_FEE_RATE:.0%}")
print(f"  DISCORD_WEBHOOK:   {'✓ set' if settings.DISCORD_WEBHOOK_URL else '✗ missing'}")


# ── 2. DB init ────────────────────────────────────────────────────────────────
section("2. DATABASE")
from backend.models.database import init_db, engine as main_engine
from backend.models.paper_trade import init_paper_db, PaperSessionLocal, PaperTrade, ModelCityAccuracy, paper_engine

print(f"  Main engine:  {main_engine.dialect.name}  ({main_engine.url.database or main_engine.url.host})")
print(f"  Paper engine: {paper_engine.dialect.name}  (same={paper_engine is main_engine})")

init_db()
init_paper_db()
print("  Tables created OK")


# ── 3. Current paper trade state ──────────────────────────────────────────────
section("3. PAPER TRADE DATABASE STATE")
db = PaperSessionLocal()
try:
    all_trades  = db.query(PaperTrade).all()
    resolved    = [t for t in all_trades if t.resolved]
    pending     = [t for t in all_trades if not t.resolved]
    wins        = [t for t in resolved if t.result == "win"]
    losses      = [t for t in resolved if t.result == "loss"]
    total_pnl   = sum(t.pnl for t in resolved if t.pnl is not None)

    print(f"  Total trades:   {len(all_trades)}")
    print(f"  Pending:        {len(pending)}")
    print(f"  Resolved:       {len(resolved)}  ({len(wins)}W / {len(losses)}L)")
    print(f"  Running P&L:    ${total_pnl:+.2f}")

    if pending:
        print(f"\n  PENDING TRADES:")
        for t in pending:
            print(f"    {t.ticker:40s}  side={t.side}  edge={t.edge:+.0%}  "
                  f"entry={t.entry_price:.2%}  contracts={t.contracts}  "
                  f"res_date={t.resolution_date}  agreement={t.agreement}")

    if resolved:
        print(f"\n  RESOLVED TRADES (last 5):")
        for t in sorted(resolved, key=lambda x: x.resolved_at or datetime.utcnow(), reverse=True)[:5]:
            icon = "✅" if t.result == "win" else "❌"
            print(f"    {icon} {t.ticker:40s}  actual={t.actual_temp}°F  "
                  f"threshold={t.threshold_f:.0f}°F  pnl=${t.pnl:+.2f}")
finally:
    db.close()


# ── 4. NWS settlement test ────────────────────────────────────────────────────
section("4. NWS SETTLEMENT (yesterday's NYC high/low)")
async def test_nws():
    from backend.data.weather import fetch_nws_observed_temperature
    yesterday = date.today() - timedelta(days=1)
    result = await fetch_nws_observed_temperature("nyc", yesterday)
    if result:
        print(f"  NYC yesterday ({yesterday}): high={result.get('high')}°F  low={result.get('low')}°F  ✓")
    else:
        print(f"  NYC yesterday ({yesterday}): ✗ NO DATA RETURNED")
        print("  This means settlement will silently skip all trades.")

asyncio.run(test_nws())


# ── 5. Simulate logging a paper trade ────────────────────────────────────────
section("5. PAPER TRADE LOG/SETTLE SIMULATION")

async def test_log_and_settle():
    from backend.core.paper_trading import log_paper_trade, settle_paper_trades
    from backend.data.weather_markets import WeatherMarket
    from backend.core.weather_signals import WeatherTradingSignal

    # Build a fake market that resolves yesterday so we can settle it immediately
    yesterday = date.today() - timedelta(days=1)
    market = WeatherMarket(
        slug="AUDIT-TEST",
        market_id="AUDIT-TEST",
        platform="kalshi",
        title="AUDIT TEST — NYC high above 50F",
        city_key="nyc",
        city_name="New York",
        target_date=yesterday,
        threshold_f=50.0,
        metric="high",
        direction="above",
        yes_price=0.50,
        no_price=0.50,
        volume=1000.0,
    )

    # Clean up any leftover from a previous audit run
    db = PaperSessionLocal()
    try:
        existing = db.query(PaperTrade).filter(PaperTrade.ticker == "AUDIT-TEST").all()
        for e in existing:
            db.delete(e)
        db.commit()
    finally:
        db.close()

    signal = WeatherTradingSignal(
        market=market,
        model_probability=0.75,
        market_probability=0.50,
        edge=0.25,
        direction="yes",
        confidence=0.75,
        kelly_fraction=0.05,
        suggested_size=50.0,
        source_probs={"gfs": 0.75, "ecmwf": 0.72},
        agreement="HIGH",
    )

    trade = log_paper_trade(signal)
    if trade is None:
        print("  ✗ log_paper_trade returned None — check for NameError in logs above")
        return
    print(f"  ✓ Trade logged: id={trade.id}  contracts={trade.contracts}  "
          f"entry={trade.entry_price:.2%}  side={trade.side}")

    # Now try to settle it
    settled = await settle_paper_trades()
    audit_settled = [t for t in settled if t.ticker == "AUDIT-TEST"]
    if audit_settled:
        t = audit_settled[0]
        print(f"  ✓ Trade settled: result={t.result}  actual={t.actual_temp}°F  pnl=${t.pnl:+.2f}")
    else:
        print("  ✗ Trade NOT settled — checking why...")
        db = PaperSessionLocal()
        try:
            pt = db.query(PaperTrade).filter(PaperTrade.ticker == "AUDIT-TEST").first()
            if pt:
                print(f"    resolved={pt.resolved}  resolution_date={pt.resolution_date}  "
                      f"today={date.today().isoformat()}")
                print(f"    date check: '{pt.resolution_date}' < '{date.today().isoformat()}' = "
                      f"{pt.resolution_date < date.today().isoformat()}")
        finally:
            db.close()

asyncio.run(test_log_and_settle())


# ── 6. P&L math verification ──────────────────────────────────────────────────
section("6. P&L MATH VERIFICATION")
entry = 0.46
contracts = 10
fee = settings.KALSHI_FEE_RATE

win_pnl  = (1.0 - entry) * contracts * (1.0 - fee)
loss_pnl = entry * contracts * -1.0

print(f"  Example: {contracts} contracts at {entry:.0%} entry")
print(f"    Win P&L:  ${win_pnl:.2f}  (${(1-entry)*contracts:.2f} gross  minus {fee:.0%} fee)")
print(f"    Loss P&L: ${loss_pnl:.2f}")
print(f"    Edge breakeven fee: {fee/(1-entry):.1%} edge required")

# Verify kelly sizing
from backend.core.probability import kelly_size
size = kelly_size(
    model_prob=0.70,
    market_price=0.50,
    direction="yes",
    bankroll=settings.INITIAL_BANKROLL,
    kelly_fraction=settings.KELLY_FRACTION,
    fee_rate=settings.KALSHI_FEE_RATE,
)
print(f"\n  Kelly size (70% model, 50% market, $1000 bankroll, 15% kelly): ${size:.2f}")
contracts_ex = int(size / 0.50)
print(f"  Contracts at 0.50: {contracts_ex}")


# ── 7. Manual settlement trigger ─────────────────────────────────────────────
section("7. MANUAL SETTLEMENT (all eligible pending trades)")

async def run_settlement():
    from backend.core.paper_trading import settle_paper_trades
    settled = await settle_paper_trades()
    if settled:
        wins = sum(1 for t in settled if t.result == "win")
        losses = sum(1 for t in settled if t.result == "loss")
        pnl = sum(t.pnl for t in settled if t.pnl is not None)
        print(f"  Settled {len(settled)} trades: {wins}W / {losses}L  P&L=${pnl:+.2f}")
        for t in settled:
            icon = "✅" if t.result == "win" else "❌"
            print(f"    {icon} {t.ticker:40s}  {t.actual_temp}°F vs {t.threshold_f:.0f}°F  "
                  f"pnl=${t.pnl:+.2f}")
    else:
        print("  No eligible trades to settle (none past resolution_date, or NWS unavailable)")

asyncio.run(run_settlement())


# ── Summary ───────────────────────────────────────────────────────────────────
section("FINAL STATE")
db = PaperSessionLocal()
try:
    all_trades = db.query(PaperTrade).filter(PaperTrade.ticker != "AUDIT-TEST").all()
    resolved   = [t for t in all_trades if t.resolved]
    pending    = [t for t in all_trades if not t.resolved]
    wins       = sum(1 for t in resolved if t.result == "win")
    losses     = sum(1 for t in resolved if t.result == "loss")
    pnl        = sum(t.pnl for t in resolved if t.pnl is not None)
    print(f"  Trades: {len(all_trades)} total  {len(pending)} pending  {len(resolved)} resolved")
    print(f"  W/L:    {wins}W / {losses}L")
    print(f"  P&L:    ${pnl:+.2f}")
finally:
    db.close()
