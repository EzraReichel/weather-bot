#!/usr/bin/env python3
"""
Smoke-tests order placement against the Kalshi demo environment.
Requires KALSHI_API_BASE_URL=https://demo-api.kalshi.co/trade-api/v2 in .env
Run: python scripts/test_order_placement.py
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
load_dotenv()

import asyncio
from weatherbot.data.kalshi_client import KalshiClient
from weatherbot.config import settings


async def main():
    print(f"  API URL:   {settings.KALSHI_API_BASE_URL}")
    print(f"  Key ID:    {settings.KALSHI_API_KEY_ID or '(not set)'}")
    print()

    assert "demo" in settings.KALSHI_API_BASE_URL, (
        "This script should only run against the demo API.\n"
        "Set KALSHI_API_BASE_URL=https://demo-api.kalshi.co/trade-api/v2 in .env"
    )

    client = KalshiClient()

    # Balance check
    balance = await client.get_balance()
    print(f"  Balance: {balance}")

    # Grab a live market ticker to test against
    markets = await client.get_markets(params={"limit": 5, "status": "open"})
    market_list = markets.get("markets", [])
    if not market_list:
        print("  No open markets found — can't test order placement")
        return

    ticker = market_list[0]["ticker"]
    print(f"  Test ticker: {ticker}")

    # Place a 1-contract limit order at 1 cent (almost certainly won't fill)
    result = await client.place_order(ticker=ticker, side="yes", count=1, yes_price=1)
    print(f"  Order placed: {result}")

    order_id = (result.get("order") or result).get("id") or (result.get("order") or result).get("order_id")
    if not order_id:
        print("  Could not extract order_id from response — skipping cancel")
        return

    # Cancel it immediately
    cancelled = await client.cancel_order(order_id)
    print(f"  Order cancelled: {cancelled}")
    print()
    print("  ✓ Order placement smoke test passed")


asyncio.run(main())
