#!/usr/bin/env python3
"""
No-network audit of Discord notifications: verifies every embed we build stays
within Discord's hard limits (so the webhook can't be silently rejected with a
400), that oversized lists get truncated, and that the 429 retry path works.

Run: python tests/test_discord_limits.py
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from datetime import date, datetime
from types import SimpleNamespace

from weatherbot.config import settings
import weatherbot.notifications.discord as d

FIELD_VALUE_MAX = 1024
FIELD_NAME_MAX  = 256
TITLE_MAX       = 256
DESC_MAX        = 4096
MAX_FIELDS      = 25

_captured = []
_fail = []


def check(label, ok):
    print(f"  {'✅' if ok else '❌'} {label}")
    if not ok:
        _fail.append(label)


class _FakeResp:
    def __init__(self, status, body=None):
        self.status_code = status
        self._body = body or {}
        self.text = str(body)

    def json(self):
        return self._body


def _validate_payload(payload):
    """Assert a webhook payload obeys Discord's documented limits."""
    for embed in payload["embeds"]:
        if "title" in embed:
            check(f"title ≤ {TITLE_MAX} ({len(embed['title'])})", len(embed["title"]) <= TITLE_MAX)
        if "description" in embed:
            check(f"description ≤ {DESC_MAX} ({len(embed['description'])})", len(embed["description"]) <= DESC_MAX)
        fields = embed.get("fields", [])
        check(f"≤ {MAX_FIELDS} fields ({len(fields)})", len(fields) <= MAX_FIELDS)
        for f in fields:
            nm, val = f.get("name", ""), f.get("value", "")
            check(f"field name ≤ {FIELD_NAME_MAX} [{nm[:20]}] ({len(nm)})", len(nm) <= FIELD_NAME_MAX)
            check(f"field value ≤ {FIELD_VALUE_MAX} [{nm[:20]}] ({len(val)})", len(val) <= FIELD_VALUE_MAX)
            check(f"field name non-empty [{nm[:20]}]", len(nm) > 0)
            check(f"field value non-empty [{nm[:20]}]", len(val) > 0)


def _fake_post_ok(url, json=None, timeout=None):
    _captured.append(json)
    _validate_payload(json)
    return _FakeResp(204)


def _trade(i, resolved=True, result="win", is_paper=True):
    return SimpleNamespace(
        ticker=f"KXHIGHNY-26JUN0{i % 9}-B{70 + i}",
        side="yes" if i % 2 else "no",
        edge=0.12 + i * 0.001,
        entry_price=0.55,
        result=result,
        pnl=(3.5 if result == "win" else -2.1),
        actual_temp=1.0 if result == "win" else 0.0,
        model_prob=0.71,
        is_paper=is_paper,
        city="nyc",
        metric="high",
        threshold_f=72.0,
        resolution_date="2026-06-01",
        created_at=datetime.utcnow(),
        resolved_at=datetime.utcnow(),
        resolved=resolved,
        contracts=5,
        agreement="HIGH",
    )


def main():
    settings.DISCORD_WEBHOOK_URL = "https://discord.com/api/webhooks/0/fake"
    orig_post = d.requests.post
    d.requests.post = _fake_post_ok

    try:
        # ── 1. Daily summary with a flood of trades ───────────────────────────
        print("1. DAILY SUMMARY — 60 logged / 60 resolved")
        logged   = [_trade(i, resolved=False) for i in range(60)]
        resolved = [_trade(i, result="win" if i % 3 else "loss") for i in range(60)]
        stats = {
            "total_pnl": 142.55, "wins": 40, "losses": 20,
            "resolved_trades": resolved,
        }
        d.send_daily_summary(
            logged_today=logged, resolved_today=resolved,
            daily_pnl=37.4, stats=stats,
            daily_brier=0.18, bankroll=1234.56, live_trading=True,
        )

        # ── 2. Settlement alert ───────────────────────────────────────────────
        print("2. SETTLEMENT ALERT")
        d.send_trade_settled_alert(_trade(1, result="loss", is_paper=False), bankroll=1200.0)

        # ── 3. Live trade alert ───────────────────────────────────────────────
        print("3. LIVE TRADE ALERT")
        sig = SimpleNamespace(
            market=SimpleNamespace(
                market_id="KXHIGHNY-26JUN01-B72", title="NY High > 72",
                city_name="New York", metric="high", direction="above",
                threshold_f=72.0, target_date=date(2026, 6, 1),
            ),
            direction="no", edge=0.15, model_probability=0.4,
            market_probability=0.55, suggested_size=40.0,
        )
        lt = SimpleNamespace(ticker="KXHIGHNY-26JUN01-B72", contracts=3,
                             entry_price=0.45, fill_price=0.45, kelly_size=13.5,
                             kalshi_order_id="ord-1")
        d.send_live_trade_alert(sig, lt)

        # ── 4. 429 retry path ─────────────────────────────────────────────────
        print("4. RATE-LIMIT (429) RETRY")
        calls = {"n": 0}

        def _post_429_then_ok(url, json=None, timeout=None):
            calls["n"] += 1
            if calls["n"] == 1:
                return _FakeResp(429, {"retry_after": 0.5})
            return _FakeResp(204)

        d.requests.post = _post_429_then_ok
        ok = d._post_embed({"title": "t", "fields": [{"name": "a", "value": "b"}]})
        check("retries after 429 and succeeds", ok is True and calls["n"] == 2)

    finally:
        d.requests.post = orig_post

    print()
    if _fail:
        print(f"❌ {len(_fail)} check(s) failed")
        return False
    print(f"✅ All limit checks passed across {len(_captured)} embed(s)")
    return True


if __name__ == "__main__":
    sys.exit(0 if main() else 1)
