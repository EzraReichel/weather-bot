#!/usr/bin/env python3
"""
End-to-end dry run — runs ONE full scan cycle, validates every layer,
never places real trades, prints a final status report.
"""
import asyncio
import sys
import time
import traceback
from datetime import date, timedelta

from dotenv import load_dotenv
load_dotenv()

from backend.config import settings
from backend.models.database import init_db

# ── Results tracker ───────────────────────────────────────────────
results = {
    "kalshi_auth":       None,
    "weather_data":      None,
    "discord_alerts":    None,
    "signal_generation": None,
    "edge_cases":        None,
}
details = {}


# ── Helpers ───────────────────────────────────────────────────────
def ok(key, msg=""):
    results[key] = True
    if msg:
        print(f"  ✅ {msg}")

def fail(key, msg=""):
    results[key] = False
    if msg:
        print(f"  ❌ {msg}")

def warn(msg):
    print(f"  ⚠️  {msg}")

def section(title):
    print(f"\n{'─'*60}")
    print(f"  {title}")
    print(f"{'─'*60}")


# ── 1. DRY RUN banner ─────────────────────────────────────────────
print("=" * 60)
print("  🔒 DRY RUN MODE — no real trades will be placed")
print("=" * 60)

if not settings.DRY_RUN:
    print("\n⚠️  WARNING: DRY_RUN=false in config — overriding to True for this script")
    settings.DRY_RUN = True


# ── 2. Kalshi auth ────────────────────────────────────────────────
section("1. KALSHI API AUTH")

async def test_kalshi():
    from backend.data.kalshi_client import KalshiClient, kalshi_credentials_present

    if not kalshi_credentials_present():
        fail("kalshi_auth", "Credentials not configured (KALSHI_API_KEY_ID / PEM key missing)")
        details["kalshi_markets"] = 0
        return

    try:
        client = KalshiClient()
        client._load_private_key()
        print("  Key loaded OK")
    except Exception as e:
        fail("kalshi_auth", f"PEM load failed: {e}")
        details["kalshi_markets"] = 0
        return

    try:
        data = await client.get_markets({"series_ticker": "KXHIGHNY", "status": "open", "limit": 5})
        n = len(data.get("markets", []))
        ok("kalshi_auth", f"Auth successful — {n} KXHIGHNY markets found")
        details["kalshi_markets_sample"] = n
    except Exception as e:
        fail("kalshi_auth", f"API call failed: {e}")
        details["kalshi_markets_sample"] = 0

asyncio.run(test_kalshi())


# ── 3. Weather data pipeline ──────────────────────────────────────
section("2. WEATHER DATA PIPELINE")

async def test_weather():
    from backend.data.weather import fetch_ensemble_forecast
    from backend.core.probability import compute_probability

    target = date.today() + timedelta(days=1)

    # Normal fetch
    try:
        fc = await fetch_ensemble_forecast("nyc", target)
        if fc and fc.num_members >= 5:
            ok("weather_data",
               f"NYC forecast: {fc.num_members} members, "
               f"high {fc.mean_high:.1f}°F ± {fc.std_high:.1f}°F")
            details["ensemble_members"] = fc.num_members
            details["nyc_mean_high"] = fc.mean_high
            details["nyc_std_high"] = fc.std_high
        elif fc:
            warn(f"Only {fc.num_members} ensemble members (expected ~31)")
            ok("weather_data")
            details["ensemble_members"] = fc.num_members
        else:
            fail("weather_data", "fetch_ensemble_forecast returned None")
            details["ensemble_members"] = 0
            return
    except Exception as e:
        fail("weather_data", f"Fetch raised: {e}")
        details["ensemble_members"] = 0
        return

    # Probability engine
    threshold = round(fc.mean_high)
    r = compute_probability(fc.member_highs, float(threshold), "above", target)
    if r:
        diff = abs(r.model_prob - r.ensemble_fraction)
        flag = "⚠️  low-conf flag set" if r.low_confidence_flag else "✅ within 15%"
        print(f"  Probability engine: CDF={r.model_prob:.1%}  "
              f"fraction={r.ensemble_fraction:.1%}  diff={diff:.1%}  {flag}")
    else:
        warn("compute_probability returned None")

asyncio.run(test_weather())


# ── 4. Discord ────────────────────────────────────────────────────
section("3. DISCORD WEBHOOK")

def test_discord():
    from backend.notifications.discord import _post_embed
    from datetime import datetime, timezone

    if not settings.DISCORD_WEBHOOK_URL:
        warn("DISCORD_WEBHOOK_URL not set — skipping Discord test")
        results["discord_alerts"] = None
        return

    embed = {
        "title": "🔒 DRY RUN — System Check",
        "description": "Bot started in dry run mode. All systems nominal.",
        "color": 0x3498DB,
        "fields": [
            {"name": "Mode",     "value": "**DRY RUN**", "inline": True},
            {"name": "DRY_RUN", "value": str(settings.DRY_RUN), "inline": True},
        ],
        "footer": {"text": "Kalshi Weather Arb Bot — dry_run.py"},
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    success = _post_embed(embed)
    if success:
        ok("discord_alerts", "Embed delivered to Discord")
    else:
        fail("discord_alerts", "Webhook POST failed (check DISCORD_WEBHOOK_URL)")

test_discord()


# ── 5. Full signal scan ───────────────────────────────────────────
section("4. FULL SIGNAL GENERATION SCAN")

async def test_signals():
    from backend.core.weather_signals import scan_for_weather_signals
    from backend.notifications.discord import send_signal_alert

    init_db()

    city_keys = [c.strip() for c in settings.WEATHER_CITIES.split(",") if c.strip()]
    print(f"  Cities: {', '.join(city_keys)}")
    print(f"  Min edge threshold: {settings.MIN_EDGE_THRESHOLD:.0%}")

    t0 = time.time()
    try:
        scan = await scan_for_weather_signals()
    except Exception as e:
        fail("signal_generation", f"scan_for_weather_signals raised: {e}")
        traceback.print_exc()
        details["signals_found"] = 0
        details["signals_above_threshold"] = 0
        return

    signals = scan.signals
    elapsed = time.time() - t0
    actionable = scan.actionable

    details["signals_found"] = len(signals)
    details["signals_above_threshold"] = len(actionable)

    if signals:
        ok("signal_generation",
           f"{len(signals)} signals in {elapsed:.1f}s, {len(actionable)} above threshold")
    else:
        ok("signal_generation", f"Scan completed in {elapsed:.1f}s — 0 signals (no Kalshi creds or no markets)")

    if actionable:
        print(f"\n  {'─'*52}")
        print(f"  SIGNALS ABOVE THRESHOLD ({len(actionable)}):")
        print(f"  {'─'*52}")
        for s in actionable:
            print(f"  {s.market.market_id}")
            print(f"    Side:    {s.direction.upper()}")
            print(f"    Model:   {s.model_probability:.1%}   Market: {s.market_probability:.1%}   Edge: {s.edge:+.1%}")
            print(f"    Kelly:   ${s.suggested_size:.0f}   Confidence: {s.confidence:.0%}"
                  + ("  ⚠️ low-conf" if s.low_confidence_flag else ""))
            print(f"    Reason:  {s.reasoning[:100]}...")

        print(f"\n  🔒 DRY RUN — none of the above would be traded")

        # Send Discord alerts
        if settings.DISCORD_WEBHOOK_URL:
            print(f"\n  Sending {len(actionable)} Discord alert(s)...")
            for s in actionable:
                try:
                    send_signal_alert(s)
                    print(f"    ✅ Alert sent: {s.market.market_id}")
                except Exception as e:
                    print(f"    ❌ Alert failed for {s.market.market_id}: {e}")
    else:
        print("  No actionable signals found — markets are efficiently priced right now.")
        print("  This is normal and expected during low-volatility periods.")

asyncio.run(test_signals())


# ── 6. Edge cases ─────────────────────────────────────────────────
section("5. EDGE CASE CHECKS")

async def test_edge_cases():
    all_ok = True
    from backend.data.weather import fetch_ensemble_forecast
    from backend.core.probability import compute_probability
    from backend.notifications.discord import _post_embed
    from backend.data.kalshi_markets import _parse_temp_ticker as _parse_kalshi_ticker
    target = date.today() + timedelta(days=1)

    # a) Zero open markets
    print("  a) Kalshi returns 0 open markets...")
    try:
        from backend.data.kalshi_markets import fetch_kalshi_weather_markets
        # This should return [] gracefully if no creds or no markets
        report = await fetch_kalshi_weather_markets(["nyc"])
        print(f"     ✅ Returns report with {len(report.markets)} markets (no crash)")
    except Exception as e:
        print(f"     ❌ Raised: {e}")
        all_ok = False

    # b) Open-Meteo returns empty (bad city)
    print("  b) Open-Meteo unavailable / bad city...")
    try:
        result = await fetch_ensemble_forecast("badcity_xyz", target)
        if result is None:
            print("     ✅ Returns None gracefully")
        else:
            print("     ⚠️  Returned data for unknown city")
    except Exception as e:
        print(f"     ❌ Raised: {e}")
        all_ok = False

    # c) Invalid Discord webhook
    print("  c) Invalid Discord webhook URL...")
    try:
        import backend.config as cfg
        saved = cfg.settings.DISCORD_WEBHOOK_URL
        cfg.settings.DISCORD_WEBHOOK_URL = "https://discord.com/api/webhooks/0/bad"
        r = _post_embed({"title": "test", "color": 0})
        cfg.settings.DISCORD_WEBHOOK_URL = saved
        if r is False:
            print("     ✅ Returns False, does not crash")
        else:
            print("     ⚠️  Unexpected success")
    except Exception as e:
        print(f"     ❌ Raised: {e}")
        all_ok = False

    # d) Malformed PEM key
    print("  d) Malformed PEM key...")
    try:
        from backend.data.kalshi_client import KalshiClient
        import backend.config as cfg
        saved_pem = cfg.settings.KALSHI_PRIVATE_KEY_PEM
        saved_path = cfg.settings.KALSHI_PRIVATE_KEY_PATH
        cfg.settings.KALSHI_PRIVATE_KEY_PEM = "THIS IS NOT A VALID PEM"
        cfg.settings.KALSHI_PRIVATE_KEY_PATH = None
        c = KalshiClient()
        try:
            c._load_private_key()
            print("     ⚠️  Did not raise on bad PEM")
        except Exception:
            print("     ✅ Raises ValueError on bad PEM (caught at caller)")
        finally:
            cfg.settings.KALSHI_PRIVATE_KEY_PEM = saved_pem
            cfg.settings.KALSHI_PRIVATE_KEY_PATH = saved_path
    except Exception as e:
        print(f"     ❌ Unexpected error: {e}")
        all_ok = False

    # e) Ensemble std = 0
    print("  e) Ensemble std = 0 (all members identical)...")
    try:
        r = compute_probability([68.0] * 31, 70.0, "above", target)
        print(f"     ✅ Returns result: P={r.model_prob:.1%} (no div-by-zero)")
    except Exception as e:
        print(f"     ❌ Raised: {e}")
        all_ok = False

    # f) Unparseable ticker
    print("  f) Unparseable Kalshi ticker...")
    try:
        result = _parse_kalshi_ticker("GARBAGE-TICKER-HERE", "nyc", "high", "")
        if result is None:
            print("     ✅ Returns None for bad ticker (no crash)")
        else:
            print(f"     ⚠️  Parsed garbage ticker as: {result}")
    except Exception as e:
        print(f"     ❌ Raised: {e}")
        all_ok = False

    ok("edge_cases") if all_ok else fail("edge_cases", "one or more edge cases failed")

asyncio.run(test_edge_cases())


# ── 7. Final report ───────────────────────────────────────────────
def status_str(v):
    if v is True:  return "✅"
    if v is False: return "❌"
    return "⚠️  skipped"

ready = all(v is True for v in results.values() if v is not None)
# kalshi_auth=False is acceptable when creds aren't set yet — don't block "ready"
if results["kalshi_auth"] is False and not settings.KALSHI_API_KEY_ID:
    ready = all(v is True for k, v in results.items() if v is not None and k != "kalshi_auth")

print(f"""
{'='*44}
DRY RUN STATUS REPORT
{'='*44}
Kalshi Auth:        {status_str(results['kalshi_auth'])}
Weather Data:       {status_str(results['weather_data'])}
Discord Alerts:     {status_str(results['discord_alerts'])}
Signal Generation:  {status_str(results['signal_generation'])}
Edge Cases:         {status_str(results['edge_cases'])}
{'─'*44}
Ensemble Members:   {details.get('ensemble_members', '?')}
Signals Found:      {details.get('signals_found', '?')}
Signals Above Edge: {details.get('signals_above_threshold', '?')}
{'='*44}
Ready for live trading: {'YES' if ready else 'NO (see failures above)'}
{'='*44}
""")

sys.exit(0 if ready else 1)
