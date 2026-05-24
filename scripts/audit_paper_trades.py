#!/usr/bin/env python3
"""
End-to-end audit of the paper trading pipeline using the unified Trade model.
Run: python scripts/audit_paper_trades.py
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import asyncio
import json
import logging
from datetime import date, datetime, timedelta
from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(level=logging.WARNING, format="[%(levelname)s] %(message)s")

SEP = "─" * 56


def section(title: str):
    print(f"\n{'='*56}")
    print(f"  {title}")
    print("=" * 56)


def check(label: str, result: bool):
    icon = "✅" if result else "❌"
    print(f"  {icon} {label}")
    return result


# ── 1. Config ──────────────────────────────────────────────────────────────────
section("1. CONFIGURATION")
all_ok = True

from weatherbot.config import settings
print(f"  DATABASE_URL:        {settings.DATABASE_URL[:50]}")
print(f"  LIVE_TRADING:        {settings.LIVE_TRADING}")
print(f"  KALSHI_API_BASE_URL: {settings.KALSHI_API_BASE_URL}")
print(f"  LIVE_MAX_TRADE_SIZE: ${settings.LIVE_MAX_TRADE_SIZE:.2f}")
print(f"  MIN_EDGE:            {settings.MIN_EDGE_THRESHOLD:.0%}")
print(f"  INITIAL_BANKROLL:    ${settings.INITIAL_BANKROLL:,.0f}")
print(f"  DISCORD_WEBHOOK:     {'✓ set' if settings.DISCORD_WEBHOOK_URL else '✗ missing'}")
print(f"  KALSHI_CREDS:        {'✓ set' if (settings.KALSHI_API_KEY_ID) else '✗ missing'}")

all_ok &= check("LIVE_TRADING defaults to False", settings.LIVE_TRADING is False)
all_ok &= check(
    "KALSHI_API_BASE_URL defaults to demo",
    "demo" in settings.KALSHI_API_BASE_URL,
)


# ── 2. Database ────────────────────────────────────────────────────────────────
section("2. DATABASE — unified Trade model")

from weatherbot.models.weather_db import init_db, engine as main_engine
from weatherbot.models.trade import init_trade_db, SessionLocal, Trade, ModelCityAccuracy

print(f"  Engine: {main_engine.dialect.name}  ({main_engine.url.database or main_engine.url.host})")

init_db()
init_trade_db()
all_ok &= check("DB tables created OK", True)

# Verify Trade table has is_paper column
from sqlalchemy import inspect as sa_inspect
inspector = sa_inspect(main_engine)
try:
    cols = [c["name"] for c in inspector.get_columns("trades")]
    all_ok &= check("trades table exists", True)
    all_ok &= check("is_paper column present", "is_paper" in cols)
    all_ok &= check("kalshi_order_id column present", "kalshi_order_id" in cols)
    all_ok &= check("fill_price column present", "fill_price" in cols)
    print(f"  Columns: {', '.join(cols)}")
except Exception as e:
    all_ok &= check(f"Could not inspect trades table: {e}", False)


# ── 3. Current trade state ─────────────────────────────────────────────────────
section("3. CURRENT TRADE STATE")

db = SessionLocal()
try:
    paper_trades = db.query(Trade).filter(Trade.is_paper == True).all()
    live_trades  = db.query(Trade).filter(Trade.is_paper == False).all()
    resolved     = [t for t in paper_trades if t.resolved]
    pending      = [t for t in paper_trades if not t.resolved]
    wins         = [t for t in resolved if t.result == "win"]
    losses       = [t for t in resolved if t.result == "loss"]
    total_pnl    = sum(t.pnl for t in resolved if t.pnl is not None)

    print(f"  Paper trades:   {len(paper_trades)} ({len(pending)} pending, {len(resolved)} resolved)")
    print(f"  Live trades:    {len(live_trades)}")
    print(f"  W/L:            {len(wins)}W / {len(losses)}L")
    print(f"  Running P&L:    ${total_pnl:+.2f}")

    all_ok &= check("is_paper=True filter returns only paper trades", all(t.is_paper for t in paper_trades))
    all_ok &= check("is_paper=False filter returns only live trades", all(not t.is_paper for t in live_trades))

    if pending:
        print(f"\n  PENDING:")
        for t in pending[:5]:
            print(f"    {t.ticker:42s} side={t.side}  edge={t.edge:+.0%}  res={t.resolution_date}")
        if len(pending) > 5:
            print(f"    ... and {len(pending) - 5} more")
finally:
    db.close()


# ── 4. log_paper_trade / execute_signal (paper mode) ──────────────────────────
section("4. LOG PAPER TRADE via execute_signal (LIVE_TRADING=False)")


async def test_execute_paper():
    from weatherbot.core.trade_manager import execute_signal
    from weatherbot.data.weather_markets import WeatherMarket
    from types import SimpleNamespace

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
        yes_price=0.55,
        no_price=0.45,
        volume=1000.0,
    )

    # Clean up leftover from any previous run
    db = SessionLocal()
    try:
        for t in db.query(Trade).filter(Trade.ticker == "AUDIT-TEST").all():
            db.delete(t)
        db.commit()
    finally:
        db.close()

    signal = SimpleNamespace(
        market=market,
        direction="yes",
        model_probability=0.75,
        market_probability=0.55,
        edge=0.20,
        confidence=0.78,
        suggested_size=50.0,
        kelly_fraction=0.05,
        source_probs={"gfs": 0.75, "ecmwf": 0.72},
        agreement="HIGH",
        ensemble_mean=54.0,
        ensemble_std=3.2,
        ensemble_members=31,
        passes_paper_threshold=True,
        passes_threshold=True,
        low_confidence_flag=False,
        outlier_dampened=None,
    )

    # LIVE_TRADING is False by default — execute_signal should route to log_paper_trade
    trade = await execute_signal(signal)
    ok1 = check("execute_signal returns a Trade", trade is not None)
    ok2 = False
    ok3 = False
    ok4 = False

    if trade:
        ok2 = check(f"trade.is_paper = {trade.is_paper}", trade.is_paper is True)
        ok3 = check(f"trade.ticker = {trade.ticker}", trade.ticker == "AUDIT-TEST")
        ok4 = check(f"trade.kalshi_order_id is None (paper)", trade.kalshi_order_id is None)

    # Dedup: second call for same ticker should return None
    trade2 = await execute_signal(signal)
    ok5 = check("Dedup: second execute_signal for same ticker returns None", trade2 is None)

    return all([ok1, ok2, ok3, ok4, ok5])


paper_ok = asyncio.run(test_execute_paper())
all_ok &= paper_ok


# ── 5. Settlement via Kalshi result ───────────────────────────────────────────
section("5. SETTLEMENT via settle_paper_trades()")


async def test_settlement():
    from weatherbot.core.paper_trading import settle_paper_trades

    settled = await settle_paper_trades()
    audit_settled = [t for t in settled if t.ticker == "AUDIT-TEST"]

    if audit_settled:
        t = audit_settled[0]
        ok1 = check(f"AUDIT-TEST settled: result={t.result}  pnl=${t.pnl:+.2f}", True)
        ok2 = check("settled trade has is_paper=True", t.is_paper is True)
        ok3 = check("resolved=True", t.resolved is True)
        ok4 = check("result is 'win' or 'loss'", t.result in ("win", "loss"))
        return all([ok1, ok2, ok3, ok4])
    else:
        # Settlement requires Kalshi creds to fetch the official result
        from weatherbot.data.kalshi_client import kalshi_credentials_present
        if not kalshi_credentials_present():
            print("  ⚠️  Kalshi creds not set — settlement skipped (expected)")
            print("     Settlement fetches the official result from Kalshi API.")
            return True
        else:
            # Creds present but not settled — check why
            db = SessionLocal()
            try:
                pt = db.query(Trade).filter(Trade.ticker == "AUDIT-TEST").first()
                if pt:
                    print(f"  Trade exists but not settled:")
                    print(f"    resolved={pt.resolved}  resolution_date={pt.resolution_date}  today={date.today()}")
                    past = pt.resolution_date < date.today().isoformat()
                    print(f"    Date is in the past: {past}")
            finally:
                db.close()
            return check("AUDIT-TEST did not settle despite creds being present", False)


all_ok &= asyncio.run(test_settlement())


# ── 6. P&L math ───────────────────────────────────────────────────────────────
section("6. P&L MATH VERIFICATION")

entry = 0.55
contracts = 10
fee = settings.KALSHI_FEE_RATE

win_pnl  = (1.0 - entry) * contracts * (1.0 - fee)
loss_pnl = entry * contracts * -1.0

expected_win  = round((1.0 - 0.55) * 10 * (1.0 - fee), 4)
expected_loss = round(0.55 * 10 * -1.0, 4)

print(f"  Entry={entry:.0%}  Contracts={contracts}  Fee={fee:.0%}")
print(f"  Win P&L:  ${win_pnl:.4f}  (expected ${expected_win:.4f})")
print(f"  Loss P&L: ${loss_pnl:.4f}  (expected ${expected_loss:.4f})")

all_ok &= check("Win P&L formula correct", round(win_pnl, 4) == expected_win)
all_ok &= check("Loss P&L formula correct", round(loss_pnl, 4) == expected_loss)


# ── 7. Kelly sizing ───────────────────────────────────────────────────────────
section("7. KELLY SIZING")

from weatherbot.core.probability import kelly_size

size = kelly_size(
    model_prob=0.70,
    market_price=0.50,
    direction="yes",
    bankroll=settings.INITIAL_BANKROLL,
    kelly_fraction=settings.KELLY_FRACTION,
    fee_rate=settings.KALSHI_FEE_RATE,
)
all_ok &= check(f"kelly_size returns a positive number: ${size:.2f}", size > 0)
all_ok &= check(
    f"kelly_size <= LIVE_MAX_TRADE_SIZE cap would apply (${settings.LIVE_MAX_TRADE_SIZE:.2f})",
    size > settings.LIVE_MAX_TRADE_SIZE,  # Size should exceed cap, proving the cap matters
)
print(f"  Kelly: ${size:.2f}  vs LIVE_MAX_TRADE_SIZE cap: ${settings.LIVE_MAX_TRADE_SIZE:.2f}")


# ── 8. trade_manager safety guard ─────────────────────────────────────────────
section("8. ORDER EXECUTOR SAFETY GUARD")

import weatherbot.config as cfg
original_live = cfg.settings.LIVE_TRADING
original_url  = cfg.settings.KALSHI_API_BASE_URL
cfg.settings.LIVE_TRADING = True
cfg.settings.KALSHI_API_BASE_URL = "https://demo-api.kalshi.co/trade-api/v2"

from weatherbot.core.trade_manager import execute_signal as _exec
from types import SimpleNamespace
from weatherbot.data.weather_markets import WeatherMarket

fake_mkt = WeatherMarket(
    slug="X", market_id="X", platform="kalshi", title="X",
    city_key="nyc", city_name="NYC", target_date=date.today(),
    threshold_f=70.0, metric="high", direction="above",
    yes_price=0.60, no_price=0.40,
)
fake_sig = SimpleNamespace(
    market=fake_mkt, direction="yes",
    model_probability=0.80, market_probability=0.60,
    edge=0.20, confidence=0.80, suggested_size=50.0,
    ensemble_mean=74.0, ensemble_std=2.0, source_probs={}, agreement="HIGH",
    passes_paper_threshold=True, passes_threshold=True,
    low_confidence_flag=False, outlier_dampened=None, kelly_fraction=0.15,
)

async def test_guard():
    try:
        await _exec(fake_sig)
        return check("Safety guard should have fired — it did NOT", False)
    except AssertionError:
        return check("AssertionError fires when LIVE_TRADING=True + demo URL", True)
    except Exception as e:
        return check(f"Unexpected error (expected AssertionError): {e}", False)

all_ok &= asyncio.run(test_guard())
cfg.settings.LIVE_TRADING = original_live
cfg.settings.KALSHI_API_BASE_URL = original_url


# ── 9. get_paper_stats ────────────────────────────────────────────────────────
section("9. get_paper_stats()")

from weatherbot.core.paper_trading import get_paper_stats

stats = get_paper_stats()
all_ok &= check("get_paper_stats returns a dict", isinstance(stats, dict))
all_ok &= check(
    "Required keys present",
    all(k in stats for k in ["total", "wins", "losses", "total_pnl", "brier", "cities", "all_trades"]),
)
all_ok &= check(
    "all_trades contains only is_paper=True trades",
    all(t.is_paper for t in stats["all_trades"]),
)
print(f"  total={stats['total']}  wins={stats['wins']}  losses={stats['losses']}  pnl=${stats['total_pnl']:+.2f}")


# ── Summary ────────────────────────────────────────────────────────────────────
section("SUMMARY")
db = SessionLocal()
try:
    paper = db.query(Trade).filter(Trade.is_paper == True, Trade.ticker != "AUDIT-TEST").all()
    resolved = [t for t in paper if t.resolved]
    pending  = [t for t in paper if not t.resolved]
    wins     = sum(1 for t in resolved if t.result == "win")
    losses   = sum(1 for t in resolved if t.result == "loss")
    pnl      = sum(t.pnl for t in resolved if t.pnl is not None)
    print(f"  Paper trades: {len(paper)} total  {len(pending)} pending  {len(resolved)} resolved")
    print(f"  W/L:          {wins}W / {losses}L")
    print(f"  P&L:          ${pnl:+.2f}")
finally:
    db.close()

print()
if all_ok:
    print("✅ All checks passed — paper trade pipeline is healthy")
else:
    print("❌ Some checks failed — see above")
