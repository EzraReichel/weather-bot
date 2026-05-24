#!/usr/bin/env python3
"""
Test Kalshi API authentication and the new order placement methods.
Run: python tests/test_kalshi_auth.py
"""
import asyncio
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
load_dotenv()

from weatherbot.config import settings
from weatherbot.data.kalshi_client import KalshiClient, kalshi_credentials_present

SEP = "─" * 52


def check(label: str, result: bool):
    icon = "✅" if result else "❌"
    print(f"  {icon} {label}")
    return result


async def main():
    all_ok = True

    # ── 1. Config ──────────────────────────────────────────────────
    print(SEP)
    print("1. CONFIGURATION")
    print(SEP)

    print(f"  API URL:          {settings.KALSHI_API_BASE_URL}")
    print(f"  Key ID:           {'set (' + settings.KALSHI_API_KEY_ID[:8] + '...)' if settings.KALSHI_API_KEY_ID else 'NOT SET'}")
    print(f"  PEM file:         {'set' if settings.KALSHI_PRIVATE_KEY_PATH else '—'}")
    print(f"  PEM inline:       {'set' if settings.KALSHI_PRIVATE_KEY_PEM else '—'}")
    print(f"  LIVE_TRADING:     {settings.LIVE_TRADING}")
    print(f"  LIVE_MAX_TRADE_SIZE: ${settings.LIVE_MAX_TRADE_SIZE:.2f}")

    all_ok &= check(
        "KALSHI_API_BASE_URL is set in settings (not hardcoded)",
        bool(settings.KALSHI_API_BASE_URL),
    )
    all_ok &= check(
        "Credentials present",
        kalshi_credentials_present(),
    )
    if not kalshi_credentials_present():
        print("\n  Skipping API tests — no credentials configured.")
        print("  Add KALSHI_API_KEY_ID + KALSHI_PRIVATE_KEY_PATH to .env")
        return all_ok

    # ── 2. Client properties ───────────────────────────────────────
    print()
    print(SEP)
    print("2. CLIENT URL PROPERTIES")
    print(SEP)

    client = KalshiClient()
    all_ok &= check(
        f"_base_url reads from settings: {client._base_url}",
        client._base_url == settings.KALSHI_API_BASE_URL,
    )
    all_ok &= check(
        f"_path_prefix extracted: {client._path_prefix}",
        client._path_prefix.startswith("/"),
    )

    # ── 3. Private key loading ─────────────────────────────────────
    print()
    print(SEP)
    print("3. PRIVATE KEY")
    print(SEP)

    try:
        client._load_private_key()
        all_ok &= check("Private key loads without error", True)
    except FileNotFoundError as e:
        all_ok &= check(f"PEM file not found: {e}", False)
    except ValueError as e:
        all_ok &= check(f"PEM format error: {e}", False)
    except Exception as e:
        all_ok &= check(f"Key load failed: {type(e).__name__}: {e}", False)

    # ── 4. Request signing (no network) ───────────────────────────
    print()
    print(SEP)
    print("4. REQUEST SIGNING (no network)")
    print(SEP)

    try:
        headers = client._sign_request("GET", "/trade-api/v2/markets")
        all_ok &= check("KALSHI-ACCESS-KEY present", "KALSHI-ACCESS-KEY" in headers)
        all_ok &= check("KALSHI-ACCESS-SIGNATURE present", "KALSHI-ACCESS-SIGNATURE" in headers)
        all_ok &= check("KALSHI-ACCESS-TIMESTAMP present", "KALSHI-ACCESS-TIMESTAMP" in headers)
        all_ok &= check(
            "Key ID matches settings",
            headers["KALSHI-ACCESS-KEY"] == settings.KALSHI_API_KEY_ID,
        )
    except Exception as e:
        all_ok &= check(f"Signing failed: {e}", False)

    # ── 5. Live API call ───────────────────────────────────────────
    print()
    print(SEP)
    print("5. API CALL — GET /markets")
    print(SEP)

    try:
        data = await client.get_markets({"status": "open", "limit": 5})
        markets = data.get("markets", [])
        all_ok &= check(f"GET /markets returned {len(markets)} markets", len(markets) >= 0)
        if markets:
            sample = markets[0]
            print(f"  Sample ticker: {sample.get('ticker')}")
    except Exception as e:
        err = str(e)
        all_ok &= check(f"GET /markets failed: {err}", False)
        if "401" in err or "403" in err:
            print("  → Auth rejected — check API key ID and PEM match")
        return all_ok

    # ── 6. Balance check ───────────────────────────────────────────
    print()
    print(SEP)
    print("6. PORTFOLIO BALANCE")
    print(SEP)

    try:
        balance = await client.get_balance()
        bal_cents = balance.get("balance", balance.get("cents", "?"))
        all_ok &= check(f"GET /portfolio/balance succeeded (balance={bal_cents}¢)", True)
    except Exception as e:
        all_ok &= check(f"Balance check failed: {e}", False)

    # ── 7. place_order / cancel_order ─────────────────────────────
    print()
    print(SEP)
    print("7. ORDER PLACEMENT (place then immediately cancel)")
    print(SEP)

    is_demo = "demo" in settings.KALSHI_API_BASE_URL
    if not is_demo:
        print("  ⚠️  KALSHI_API_BASE_URL is NOT the demo API")
        print("  Skipping order placement test on non-demo URL to avoid real spend.")
        print("  Set KALSHI_API_BASE_URL=https://demo-api.kalshi.co/trade-api/v2 to test.")
    else:
        # Find an open market to place against
        try:
            data = await client.get_markets({"status": "open", "limit": 10})
            open_markets = data.get("markets", [])
            if not open_markets:
                print("  No open markets found — skipping order test")
            else:
                ticker = open_markets[0]["ticker"]
                print(f"  Using ticker: {ticker}")

                # Place a 1-contract limit at 1 cent (almost certainly won't fill)
                result = await client.place_order(
                    ticker=ticker, side="yes", count=1, yes_price=1
                )
                order = result.get("order", result)
                order_id = order.get("id") or order.get("order_id")
                all_ok &= check(f"place_order succeeded (order_id={order_id})", bool(order_id))

                if order_id:
                    cancelled = await client.cancel_order(order_id)
                    all_ok &= check("cancel_order succeeded", True)

        except Exception as e:
            all_ok &= check(f"Order test failed: {e}", False)

    # ── 8. Safety assertion in trade_manager ──────────────────────
    print()
    print(SEP)
    print("8. LIVE TRADING SAFETY ASSERTIONS")
    print(SEP)

    # Verify the demo-URL guard in execute_signal triggers correctly
    import weatherbot.config as cfg
    original_live = cfg.settings.LIVE_TRADING
    original_url = cfg.settings.KALSHI_API_BASE_URL

    cfg.settings.LIVE_TRADING = True
    cfg.settings.KALSHI_API_BASE_URL = "https://demo-api.kalshi.co/trade-api/v2"

    from weatherbot.core.trade_manager import execute_signal
    from types import SimpleNamespace
    from datetime import date

    fake_signal = SimpleNamespace(
        market=SimpleNamespace(
            market_id="TEST", city_key="nyc", metric="high",
            threshold_f=70.0, direction="above",
            target_date=date.today(), yes_price=0.60, no_price=0.40,
            city_name="NYC", title="test",
        ),
        direction="yes",
        model_probability=0.80,
        market_probability=0.60,
        edge=0.20,
        confidence=0.80,
        suggested_size=50.0,
        ensemble_mean=74.0,
        ensemble_std=2.0,
        source_probs={},
        agreement="HIGH",
        passes_paper_threshold=True,
        passes_threshold=True,
    )

    try:
        await execute_signal(fake_signal)
        all_ok &= check("Safety assertion should have fired — it did NOT", False)
    except AssertionError as e:
        all_ok &= check(f"Safety assertion fires on demo URL with LIVE_TRADING=True: {e}", True)
    except Exception as e:
        all_ok &= check(f"Unexpected error: {e}", False)
    finally:
        cfg.settings.LIVE_TRADING = original_live
        cfg.settings.KALSHI_API_BASE_URL = original_url

    print()
    print(SEP)
    if all_ok:
        print("✅ All Kalshi auth tests passed")
    else:
        print("❌ Some tests failed — see above")
    return all_ok


if __name__ == "__main__":
    ok = asyncio.run(main())
    sys.exit(0 if ok else 1)
