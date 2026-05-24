# Live Trading Testing Guide

## Overview

The bot has two trading modes controlled by a single flag in `.env`:

- `LIVE_TRADING=false` — paper trades only (default, safe)
- `LIVE_TRADING=true` — real Kalshi orders placed

A safety assertion in `order_executor.py` will hard-crash if you set `LIVE_TRADING=true` but forget to also point the URL at the prod API — you can't accidentally go live on the demo environment.

---

## Step 1: Test Auth Against Demo (No Real Money)

Create a demo account at [demo.kalshi.co](https://demo.kalshi.co), generate an API key, and download the PEM file.

Add to `.env`:
```
KALSHI_API_KEY_ID=your-demo-key-id
KALSHI_PRIVATE_KEY_PATH=./kalshi_demo_key.pem
KALSHI_API_BASE_URL=https://demo-api.kalshi.co/trade-api/v2
LIVE_TRADING=false
```

Run the auth test:
```bash
python tests/test_kalshi_auth.py
```

This will:
- Verify credentials load and sign correctly
- Hit GET /markets on the demo API
- Check your demo balance
- Place a 1-contract limit order at 1¢ (won't fill) then cancel it immediately

---

## Step 2: Run the Bot in Paper Mode Against Demo API

Keep `LIVE_TRADING=false` but point at the demo API URL. This lets you confirm the full scan → signal → paper trade → Discord alert pipeline works end-to-end without any real orders ever being placed.

```bash
python main.py
```

Watch logs and Discord for paper trade alerts. Run the audit at any time:
```bash
python scripts/audit_paper_trades.py
```

---

## Step 3: Smoke Test Live Order Placement on Demo

Still on demo, still no real money. Flip the trading mode:

```env
LIVE_TRADING=true
KALSHI_API_BASE_URL=https://demo-api.kalshi.co/trade-api/v2
```

**The bot will refuse to start** — the safety assertion in `order_executor.py` fires:
```
AssertionError: LIVE_TRADING=true but KALSHI_API_BASE_URL points at the demo API
```

This is intentional. The guard exists so you must explicitly set the prod URL to go live. To smoke test live order flow on demo, use the test script instead:

```bash
python scripts/test_order_placement.py
```

This script has its own guard that allows the demo URL and will place + cancel a real (fake-money) order.

---

## Step 4: Go Live (Real Money)

Only do this after Steps 1–3 pass cleanly.

Switch to prod credentials (separate API key from your real Kalshi account):

```env
KALSHI_API_KEY_ID=your-prod-key-id
KALSHI_PRIVATE_KEY_PATH=./kalshi_prod_key.pem
KALSHI_API_BASE_URL=https://api.kalshi.com/trade-api/v2
LIVE_TRADING=true
LIVE_MAX_TRADE_SIZE=5.0
```

`LIVE_MAX_TRADE_SIZE` caps every order at $5 regardless of what Kelly sizing says. Start small — raise it only after you've watched a few real cycles settle correctly.

---

## Safety Checks Summary

| Scenario | What happens |
|---|---|
| `LIVE_TRADING=false` | `execute_signal` calls `log_paper_trade` — no API call made |
| `LIVE_TRADING=true` + demo URL | `AssertionError` — bot refuses to run |
| `LIVE_TRADING=true` + prod URL | Real order placed, capped at `LIVE_MAX_TRADE_SIZE` |
| Order placed, Discord webhook missing | Order still placed — only notification is skipped |

---

## Relevant Files

| File | Purpose |
|---|---|
| `weatherbot/core/order_executor.py` | Branches paper vs live, safety assertions |
| `weatherbot/data/kalshi_client.py` | `place_order()`, `cancel_order()` |
| `weatherbot/config.py` | `LIVE_TRADING`, `KALSHI_API_BASE_URL`, `LIVE_MAX_TRADE_SIZE` |
| `scripts/test_order_placement.py` | Demo smoke test — place + cancel |
| `tests/test_kalshi_auth.py` | Auth, signing, balance, order placement |
| `scripts/audit_paper_trades.py` | Full paper pipeline health check |
