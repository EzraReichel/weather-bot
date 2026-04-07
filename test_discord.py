#!/usr/bin/env python3
"""Test Discord webhook with a sample signal embed."""
import sys
from datetime import datetime, timezone

from dotenv import load_dotenv
load_dotenv()

from backend.config import settings


def main():
    print("─" * 50)
    print("TEST: Discord Webhook")
    print("─" * 50)

    if not settings.DISCORD_WEBHOOK_URL:
        print("❌ DISCORD_WEBHOOK_URL not set in .env")
        return False

    print(f"Webhook URL: {settings.DISCORD_WEBHOOK_URL[:50]}...")

    import requests

    # ── 1. Send a realistic-looking test signal embed ─────────────
    print("\nSending test signal embed...")
    embed = {
        "title": "🧪 TEST SIGNAL — NYC High Temp",
        "description": "New York — HIGH temp **above** 72°F on 2026-04-08",
        "color": 0x2ECC71,
        "fields": [
            {"name": "Ticker",       "value": "`KXHIGHNY-26APR08-B72`",  "inline": True},
            {"name": "Side",         "value": "**BUY YES**",              "inline": True},
            {"name": "Edge",         "value": "**+17.0%**",               "inline": True},
            {"name": "Model Prob",   "value": "82.0%",                    "inline": True},
            {"name": "Market Price", "value": "65.0¢",                    "inline": True},
            {"name": "Kelly Size",   "value": "$45",                      "inline": True},
            {"name": "Confidence",   "value": "78%",                      "inline": False},
            {"name": "Forecast",     "value": "Mean: 74.2°F  |  Std: 2.1°F  |  Members: 31", "inline": False},
        ],
        "footer": {"text": "This is a test — no real trade  |  Kalshi Weather Arb Bot"},
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    try:
        resp = requests.post(
            settings.DISCORD_WEBHOOK_URL,
            json={"embeds": [embed]},
            timeout=10,
        )
        if resp.status_code in (200, 204):
            print("✅ Discord alert sent successfully")
        else:
            print(f"❌ Discord returned {resp.status_code}: {resp.text[:200]}")
            return False
    except requests.exceptions.ConnectionError:
        print("❌ Connection error — check DISCORD_WEBHOOK_URL is valid")
        return False
    except requests.exceptions.Timeout:
        print("❌ Request timed out")
        return False
    except Exception as e:
        print(f"❌ Discord failed: {type(e).__name__}: {e}")
        return False

    # ── 2. Edge case: bad URL ─────────────────────────────────────
    print("\nEdge case: invalid webhook URL (should log error, not crash)...")
    from backend.notifications import discord as disc_module
    original_url = settings.DISCORD_WEBHOOK_URL

    # Temporarily monkeypatch
    import backend.config as cfg
    original = cfg.settings.DISCORD_WEBHOOK_URL
    cfg.settings.DISCORD_WEBHOOK_URL = "https://discord.com/api/webhooks/invalid/bad"

    try:
        result = disc_module._post_embed({"title": "test", "color": 0})
        if result is False:
            print("✅ Returns False gracefully on bad URL (does not crash)")
        else:
            print("⚠️  Unexpected success on bad URL")
    except Exception as e:
        print(f"❌ Raised exception instead of returning False: {e}")
    finally:
        cfg.settings.DISCORD_WEBHOOK_URL = original

    print(f"\n✅ Discord webhook working")
    return True


if __name__ == "__main__":
    ok = main()
    sys.exit(0 if ok else 1)
