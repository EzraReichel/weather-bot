#!/usr/bin/env python3
"""
Place a real ~$1 live trade to exercise the full pipeline:
  auth → API → order fill → DB record

Run: python scripts/test_live_trade.py
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import os
from dotenv import dotenv_values

# dotenv can't parse the multi-line PEM in .env — load only clean single-line vars
_env = {k: v for k, v in dotenv_values(".env").items() if v and "\n" not in v}
os.environ.update(_env)
# Ensure the file-based key path is used; ignore any mangled PEM fragments
os.environ.pop("KALSHI_PRIVATE_KEY_PEM", None)

import asyncio
import json
from datetime import datetime, date, timedelta

from weatherbot.data.kalshi_client import KalshiClient
from weatherbot.config import settings
from weatherbot.models.trade import SessionLocal, Trade, init_trade_db


TARGET_DOLLARS = 1.00


async def main():
    print(f"API:    {settings.KALSHI_API_BASE_URL}")
    print(f"Key:    {settings.KALSHI_API_KEY_ID}")
    print()

    client = KalshiClient()

    # ── Balance check ─────────────────────────────────────────────────────────
    balance_data = await client.get_balance()
    if "balance_dollars" in balance_data:
        balance = float(balance_data["balance_dollars"])
    else:
        balance = balance_data.get("balance", 0) / 100.0
    print(f"Balance: ${balance:.2f}")
    if balance < TARGET_DOLLARS:
        print(f"Insufficient balance (${balance:.2f}) — need ${TARGET_DOLLARS:.2f}")
        return
    print()

    # ── Find a tradeable market ───────────────────────────────────────────────
    print("Fetching open markets...")
    markets_resp = await client.get_markets(params={"limit": 50, "status": "open"})
    market_list = markets_resp.get("markets", [])
    print(f"Found {len(market_list)} open markets")

    # Only trade weather markets — KXHIGH*, KXLOW*, KXRAIN*
    WEATHER_PREFIXES = ("KXHIGH", "KXLOW", "KXRAIN")

    def is_weather(m):
        ticker = m.get("ticker", "").upper()
        return any(ticker.startswith(p) for p in WEATHER_PREFIXES)

    weather_markets = [m for m in market_list if is_weather(m)]
    print(f"Weather markets available: {len(weather_markets)}")

    if not weather_markets:
        print("No weather markets found in first 50 results — fetching more...")
        resp2 = await client.get_markets(params={"limit": 200, "status": "open"})
        weather_markets = [m for m in resp2.get("markets", []) if is_weather(m)]
        print(f"Weather markets after wider fetch: {len(weather_markets)}")

    chosen = None
    for m in weather_markets:
        ask_dol_str = m.get("yes_ask_dollars") or m.get("yes_ask")
        if not ask_dol_str:
            continue
        try:
            ask_dol = float(ask_dol_str) if isinstance(ask_dol_str, str) else ask_dol_str / 100.0
        except (ValueError, TypeError):
            continue
        ask_size = float(m.get("yes_ask_size_fp") or m.get("yes_ask_size") or 0)
        if 0.05 <= ask_dol <= 0.90 and ask_size >= 1:
            chosen = m
            chosen["_yes_ask_dol"] = ask_dol
            break

    if not chosen:
        # Fallback: any weather market with an ask
        for m in weather_markets:
            ask_dol_str = m.get("yes_ask_dollars") or m.get("yes_ask")
            if ask_dol_str:
                try:
                    ask_dol = float(ask_dol_str) if isinstance(ask_dol_str, str) else ask_dol_str / 100.0
                except (ValueError, TypeError):
                    continue
                if ask_dol > 0:
                    chosen = m
                    chosen["_yes_ask_dol"] = ask_dol
                    break

    if not chosen:
        print("No suitable market found — aborting")
        print("Market price fields:", [(m.get("ticker"), m.get("yes_ask_dollars"), m.get("yes_ask_size_fp")) for m in market_list[:5]])
        return

    ticker      = chosen["ticker"]
    yes_ask_dol = chosen["_yes_ask_dol"]
    yes_ask     = round(yes_ask_dol * 100)   # cents (integer)
    contracts   = max(1, round(TARGET_DOLLARS / yes_ask_dol))
    total_cost  = contracts * yes_ask_dol

    # Try to parse market close date for resolution_date
    close_time = chosen.get("close_time") or chosen.get("expiration_time") or ""
    try:
        resolution_date = close_time[:10]   # "YYYY-MM-DD"
    except Exception:
        resolution_date = (date.today() + timedelta(days=1)).isoformat()

    print(f"\nMarket:      {ticker}")
    print(f"Title:       {chosen.get('title', '(no title)')}")
    print(f"YES ask:     {yes_ask}¢  (${yes_ask_dol:.2f})")
    print(f"Contracts:   {contracts}")
    print(f"Total cost:  ${total_cost:.2f}")
    print(f"Resolve by:  {resolution_date}")
    print()

    # ── Place the order ───────────────────────────────────────────────────────
    print("Placing order...")
    result = await client.place_order(
        ticker=ticker,
        side="yes",
        count=contracts,
        yes_price=yes_ask,
    )
    print(f"API response: {json.dumps(result, indent=2)}")
    print()

    order      = result.get("order", result)
    order_id   = order.get("id") or order.get("order_id")
    fill_price_raw = order.get("yes_price") or order.get("fill_price")
    fill_price = (fill_price_raw / 100.0) if fill_price_raw else yes_ask_dol
    status     = order.get("status", "unknown")

    print(f"Order ID:   {order_id}")
    print(f"Status:     {status}")
    print(f"Fill price: ${fill_price:.2f}")
    print()

    # ── Record in DB ──────────────────────────────────────────────────────────
    init_trade_db()
    db = SessionLocal()
    try:
        trade = Trade(
            is_paper        = False,
            ticker          = ticker,
            city            = "test",
            metric          = "test",
            threshold_f     = 0.0,
            side            = "yes",
            market_direction= "above",
            agreement       = "HIGH",
            model_probs     = json.dumps({"test": 1.0}),
            model_prob      = yes_ask_dol,
            market_price    = yes_ask_dol,
            edge            = 0.0,
            confidence      = 1.0,
            kelly_size      = total_cost,
            contracts       = contracts,
            entry_price     = yes_ask_dol,
            fill_price      = fill_price,
            kalshi_order_id = order_id,
            forecast_mean   = 0.0,
            forecast_std    = 0.0,
            resolution_date = resolution_date,
            resolved        = False,
        )
        db.add(trade)
        db.commit()
        db.refresh(trade)
        print(f"DB row ID:  {trade.id}")
        print()
        print(f"Pipeline test complete — trade #{trade.id} is live on Kalshi.")
        print(f"  Ticker:    {ticker}")
        print(f"  Side:      YES x{contracts} @ ${fill_price:.2f}")
        print(f"  Cost:      ~${total_cost:.2f}")
        print(f"  Order ID:  {order_id}")
        print(f"  Resolves:  {resolution_date}")
    finally:
        db.close()


asyncio.run(main())
