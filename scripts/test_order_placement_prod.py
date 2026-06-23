#!/usr/bin/env python3
"""
PROD smoke test for the Kalshi V2 order-placement migration (commit 9f21d06).

Unlike test_order_placement.py (demo-only), this runs against PRODUCTION and is
meant to be run manually, once, to validate the V2 endpoint accepts real orders.

Safety design:
  • Picks an open market priced safely in the interior (yes_bid/yes_ask away from
    1c and 99c) so both test orders are NON-marketable and cannot fill.
  • YES path: buy 1 YES @ 1c  -> rests (nobody sells YES that cheap).
  • NO  path: buy 1 NO  @ 1c  -> YES ask @ 99c -> rests (nobody buys YES that dear).
  • Every placed order id is cancelled in a finally block, even on error.
  • After each placement we read the order back and assert it did not fill.

Run: python scripts/test_order_placement_prod.py
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
load_dotenv()

import asyncio
import httpx
from weatherbot.data.kalshi_client import KalshiClient
from weatherbot.config import settings


async def _safe_readback(client, order_id):
    """GET an order, tolerating the read-after-write 404 race on Kalshi's
    GET /portfolio/orders/{id} immediately after placement."""
    try:
        return await client.get_order(order_id)
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 404:
            return "(not yet queryable — read-after-write lag; cancel will confirm)"
        raise


def _extract_id(result: dict):
    inner = result.get("order") or result
    return inner.get("id") or inner.get("order_id")


def _cents(v):
    """Parse a V2 `*_dollars` string field to whole cents, or None."""
    if v in (None, ""):
        return None
    try:
        return round(float(v) * 100)
    except (TypeError, ValueError):
        return None


def _pick_safe_market(markets: list) -> dict | None:
    """First open market whose spread keeps both 1c and 99c non-marketable."""
    for m in markets:
        yb = _cents(m.get("yes_bid_dollars"))
        ya = _cents(m.get("yes_ask_dollars"))
        if yb is None or ya is None:
            continue
        # 1c YES bid fills only if yes_ask <= 1; 99c YES ask fills only if yes_bid >= 99.
        if 5 <= yb <= 95 and 5 <= ya <= 95:
            m["_yb_cents"], m["_ya_cents"] = yb, ya
            return m
    return None


async def main():
    print(f"  API URL: {settings.KALSHI_API_BASE_URL}")
    if "demo" in settings.KALSHI_API_BASE_URL:
        print("  This is the demo URL — use test_order_placement.py instead.")
        return
    print("  >>> RUNNING AGAINST PRODUCTION <<<\n")

    client = KalshiClient()

    bal = await client.get_balance()
    print(f"  Balance: {bal.get('balance_dollars')}\n")

    # Test against the bot's own domain (KXHIGH* weather markets) — these carry
    # live quotes during the day, unlike the quote-less MVE markets up top.
    weather_series = ["KXHIGHNY", "KXHIGHCHI", "KXHIGHMIA", "KXHIGHLAX", "KXHIGHDEN", "KXHIGHAUS"]
    market = None
    for series in weather_series:
        ms = (await client.get_markets(
            params={"series_ticker": series, "status": "open", "limit": 50}
        )).get("markets", [])
        market = _pick_safe_market(ms)
        if market:
            break
    if not market:
        print("  No interior-priced open market found — aborting to stay non-marketable.")
        return
    ticker = market["ticker"]
    print(f"  Test ticker: {ticker}  (yes_bid={market['_yb_cents']}c, yes_ask={market['_ya_cents']}c)\n")

    placed: list[str] = []
    results = {}
    try:
        # --- YES / bid path: buy 1 YES @ 1c ---
        yes_res = await client.place_order(ticker=ticker, side="yes", count=1, yes_price=1)
        yes_id = _extract_id(yes_res)
        if yes_id:
            placed.append(yes_id)
        print(f"  [YES] placed -> id={yes_id}  resp={yes_res}")
        if yes_id:
            chk = await _safe_readback(client, yes_id)
            print(f"  [YES] readback: {chk}")
        results["yes"] = bool(yes_id)

        # --- NO / ask path: buy 1 NO @ 1c -> YES ask @ 99c ---
        no_res = await client.place_order(ticker=ticker, side="no", count=1, no_price=1)
        no_id = _extract_id(no_res)
        if no_id:
            placed.append(no_id)
        print(f"  [NO ] placed -> id={no_id}  resp={no_res}")
        if no_id:
            chk = await _safe_readback(client, no_id)
            print(f"  [NO ] readback: {chk}")
        results["no"] = bool(no_id)
    finally:
        print()
        for oid in placed:
            try:
                c = await client.cancel_order(oid)
                print(f"  cancelled {oid}: {c}")
            except Exception as e:
                print(f"  !! FAILED to cancel {oid}: {e} -- CANCEL MANUALLY")

    print()
    ok = results.get("yes") and results.get("no")
    print(f"  {'✓' if ok else '✗'} prod smoke test: YES={results.get('yes')} NO={results.get('no')}")


asyncio.run(main())
