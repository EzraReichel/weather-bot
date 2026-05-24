#!/usr/bin/env python3
"""
Test Discord notifications — paper trade alert, live trade alert, graceful failure.
Run: python tests/test_discord.py
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from datetime import date, datetime, timezone
from types import SimpleNamespace
from dotenv import load_dotenv
load_dotenv()

from weatherbot.config import settings
from weatherbot.notifications.discord import (
    _post_embed,
    send_paper_trade_alert,
    send_live_trade_alert,
)

SEP = "─" * 52


def _fake_signal(direction="yes"):
    market = SimpleNamespace(
        market_id="KXHIGHNY-26JUN01-B72",
        title="New York High Temp above 72°F",
        city_name="New York City",
        city_key="nyc",
        metric="high",
        direction="above",
        threshold_f=72.0,
        target_date=date(2026, 6, 1),
    )
    return SimpleNamespace(
        market=market,
        direction=direction,
        confidence=0.78,
        low_confidence_flag=False,
        edge=0.17,
        model_probability=0.82,
        market_probability=0.65,
        suggested_size=45.0,
        source_probs={"gfs": 0.83, "ecmwf": 0.80, "gem": 0.79, "nws": 0.78},
        agreement="HIGH",
        outlier_dampened=None,
        ensemble_mean=74.2,
        ensemble_std=2.1,
        ensemble_members=31,
    )


def _fake_paper_trade():
    return SimpleNamespace(
        ticker="KXHIGHNY-26JUN01-B72",
        is_paper=True,
        contracts=5,
        entry_price=0.65,
        fill_price=None,
        kelly_size=45.0,
        kalshi_order_id=None,
    )


def _fake_live_trade():
    return SimpleNamespace(
        ticker="KXHIGHNY-26JUN01-B72",
        is_paper=False,
        contracts=1,
        entry_price=0.65,
        fill_price=0.65,
        kelly_size=5.0,
        kalshi_order_id="ord-abc123",
    )


def check(label: str, result: bool):
    icon = "✅" if result else "❌"
    print(f"  {icon} {label}")
    return result


def main():
    all_ok = True

    if not settings.DISCORD_WEBHOOK_URL:
        print("⚠️  DISCORD_WEBHOOK_URL not set — webhook tests will be skipped")
        print("   Set it in .env to test actual Discord delivery")
        print()

    # ── 1. send_paper_trade_alert ──────────────────────────────────
    print(SEP)
    print("1. PAPER TRADE ALERT")
    print(SEP)

    signal = _fake_signal("yes")
    trade = _fake_paper_trade()

    if settings.DISCORD_WEBHOOK_URL:
        ok = send_paper_trade_alert(signal, trade)
        all_ok &= check("Paper trade alert sent (check Discord)", ok)
    else:
        # Verify it returns False when no webhook set, doesn't crash
        ok = send_paper_trade_alert(signal, trade)
        all_ok &= check("Returns False gracefully when no webhook configured", ok is False)

    # ── 2. send_paper_trade_alert — NO side ───────────────────────
    print()
    print(SEP)
    print("2. PAPER TRADE ALERT — NO side (flipped probabilities)")
    print(SEP)

    signal_no = _fake_signal("no")
    if settings.DISCORD_WEBHOOK_URL:
        ok = send_paper_trade_alert(signal_no, trade)
        all_ok &= check("NO-side paper alert sent (probabilities should be flipped)", ok)
    else:
        try:
            send_paper_trade_alert(signal_no, trade)
            all_ok &= check("NO-side alert does not crash", True)
        except Exception as e:
            all_ok &= check(f"NO-side alert crashed: {e}", False)

    # ── 3. send_live_trade_alert ───────────────────────────────────
    print()
    print(SEP)
    print("3. LIVE TRADE ALERT (orange color, 'real money' footer)")
    print(SEP)

    live_trade = _fake_live_trade()

    if settings.DISCORD_WEBHOOK_URL:
        ok = send_live_trade_alert(signal, live_trade)
        all_ok &= check("Live trade alert sent (check Discord — should be orange)", ok)
    else:
        try:
            result = send_live_trade_alert(signal, live_trade)
            all_ok &= check("Returns False gracefully when no webhook configured", result is False)
        except Exception as e:
            all_ok &= check(f"Live trade alert crashed: {e}", False)

    # ── 4. Graceful failure on bad webhook URL ─────────────────────
    print()
    print(SEP)
    print("4. GRACEFUL FAILURE — bad webhook URL")
    print(SEP)

    original = settings.DISCORD_WEBHOOK_URL
    settings.DISCORD_WEBHOOK_URL = "https://discord.com/api/webhooks/0/bad_token"
    try:
        result = _post_embed({"title": "test", "color": 0})
        all_ok &= check("_post_embed returns False on bad URL (does not raise)", result is False)
    except Exception as e:
        all_ok &= check(f"_post_embed raised instead of returning False: {e}", False)
    finally:
        settings.DISCORD_WEBHOOK_URL = original

    # ── 5. Returns False when no URL set ──────────────────────────
    print()
    print(SEP)
    print("5. RETURNS FALSE — no webhook URL")
    print(SEP)

    original = settings.DISCORD_WEBHOOK_URL
    settings.DISCORD_WEBHOOK_URL = None
    try:
        r1 = send_paper_trade_alert(signal, trade)
        r2 = send_live_trade_alert(signal, live_trade)
        all_ok &= check("send_paper_trade_alert returns False when no URL", r1 is False)
        all_ok &= check("send_live_trade_alert returns False when no URL", r2 is False)
    finally:
        settings.DISCORD_WEBHOOK_URL = original

    print()
    print(SEP)
    if all_ok:
        print("✅ All Discord tests passed")
    else:
        print("❌ Some tests failed — see above")
    return all_ok


if __name__ == "__main__":
    ok = main()
    sys.exit(0 if ok else 1)
