#!/usr/bin/env python3
"""Test Kalshi API authentication."""
import asyncio
import os
import sys

# Load .env
from dotenv import load_dotenv
load_dotenv()

from backend.config import settings
from backend.data.kalshi_client import KalshiClient, kalshi_credentials_present


async def main():
    print("─" * 50)
    print("TEST: Kalshi API Authentication")
    print("─" * 50)

    # Check credentials present
    key_id = settings.KALSHI_API_KEY_ID
    has_pem_file = bool(settings.KALSHI_PRIVATE_KEY_PATH)
    has_pem_inline = bool(settings.KALSHI_PRIVATE_KEY_PEM)

    print(f"API Key ID:      {'✅ set' if key_id else '❌ missing (KALSHI_API_KEY_ID)'}")
    print(f"PEM file path:   {'✅ set' if has_pem_file else '— not set'}")
    print(f"PEM inline:      {'✅ set' if has_pem_inline else '— not set'}")

    if not key_id:
        print("\n❌ KALSHI_API_KEY_ID not configured. Add it to .env")
        return False

    if not has_pem_file and not has_pem_inline:
        print("\n❌ No PEM key configured.")
        print("   Set KALSHI_PRIVATE_KEY_PATH (path to .pem file)")
        print("   or KALSHI_PRIVATE_KEY_PEM (inline PEM string, \\n-escaped)")
        return False

    # Try loading the private key
    print("\nLoading private key...")
    try:
        client = KalshiClient()
        client._load_private_key()
        print("✅ Private key loaded successfully")
    except FileNotFoundError as e:
        print(f"❌ PEM file not found: {e}")
        print("   Check KALSHI_PRIVATE_KEY_PATH points to a valid file")
        return False
    except ValueError as e:
        print(f"❌ PEM key error: {e}")
        return False
    except Exception as e:
        print(f"❌ Failed to load key: {type(e).__name__}: {e}")
        print("   Make sure your PEM is a valid RSA private key")
        print("   If using inline PEM, ensure \\n is used for newlines")
        return False

    # Test signing (no network needed)
    print("Testing request signing...")
    try:
        headers = client._sign_request("GET", "/trade-api/v2/markets")
        assert "KALSHI-ACCESS-KEY" in headers
        assert "KALSHI-ACCESS-SIGNATURE" in headers
        assert "KALSHI-ACCESS-TIMESTAMP" in headers
        print("✅ Request signing works")
    except Exception as e:
        print(f"❌ Signing failed: {e}")
        return False

    # Hit the real API
    print("Hitting Kalshi GET /markets (series=KXHIGHNY)...")
    try:
        data = await client.get_markets({"series_ticker": "KXHIGHNY", "status": "open", "limit": 10})
        markets = data.get("markets", [])
        print(f"✅ Kalshi auth successful — found {len(markets)} open KXHIGHNY markets")
        if markets:
            print(f"   Sample: {markets[0].get('ticker')}  yes_ask={markets[0].get('yes_ask')}¢")
        return True
    except Exception as e:
        err = str(e)
        print(f"❌ Kalshi API request failed: {err}")
        if "401" in err or "403" in err:
            print("   Auth rejected — check your API key ID and that the PEM matches it")
        elif "timeout" in err.lower():
            print("   Request timed out — check network/firewall")
        else:
            print("   Unexpected error — see above")
        return False


if __name__ == "__main__":
    ok = asyncio.run(main())
    sys.exit(0 if ok else 1)
