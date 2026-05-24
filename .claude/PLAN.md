# Refactor & Live Trading Plan

## Phase 1: Kill the Dead Code
*Goal: remove everything that isn't part of the active weather/paper-trading system*

1. Delete `weatherbot/core/settlement.py` entirely
2. In `weatherbot/models/database.py`: remove `Trade`, `BtcPriceSnapshot`, `AILog`, `BotState` models and the `ensure_schema()` migration for them. Update the file docstring.
3. In `weatherbot/models/database.py`: remove `ai_calls_made` and `ai_cost_usd` from `ScanLog`
4. In `weatherbot/data/weather_markets.py`: remove `fetch_polymarket_weather_markets()`
5. Grep for anything that imported from `settlement.py` or those dead models and remove those imports

**After every deletion, run:**
```bash
python -c "import weatherbot; print('imports OK')"
python main.py &; sleep 3; curl -s http://localhost:8080/health; kill %1
```

**Grep to confirm something is safe to delete before touching it:**
```bash
grep -r "settlement\|BtcPriceSnapshot\|AILog\|BotState\|fetch_polymarket" weatherbot/ --include="*.py"
```

**Note:** When deleting `Trade` from `database.py`, delete `ensure_schema()` and its call in `init_db()` at the same time — otherwise you'll get a runtime error on first DB access.

---

## Phase 2: Consolidate Config
*Goal: one canonical place for every value*

1. Create `weatherbot/cities.json` with all 18+ cities, each having `name`, `lat`, `lon`, `enabled`. Set Miami and LA to `"enabled": false`
2. Remove `CITY_CONFIG` from `weatherbot/data/weather.py` and replace with a loader that reads `cities.json`
3. Remove `WEATHER_CITIES` from `config.py` — the JSON is now the source of truth. Add `CITY_OVERRIDE: str = ""` for dev convenience (e.g. `CITY_OVERRIDE=nyc` in `.env` to only scan one city locally)
4. Move `MIN_ASK_SIZE` and `MIN_VOLUME_24H` from `weatherbot/data/kalshi_markets.py` into `config.py`
5. Move inline magic numbers in `weatherbot/core/weather_signals.py` to named module-level constants:
   - `8.0` → `MODEL_DIVERGENCE_THRESHOLD`
   - `6` (hours) → `OBS_WINDOW_HOURS`
   - `1.5` → `CLIMATOLOGY_DEVIATION_MAX`
   - `4.0` → `COLD_DAY_MARGIN`
   - `0.85` → `COLD_DAY_NWS_MIN`
   - `0.30` → `YES_ENTRY_FLOOR`
   - `0.05` → `RAIN_ENTRY_FLOOR`
6. Collapse `SIMULATION_MODE` and `DRY_RUN` into a single `LIVE_TRADING: bool = False` in `config.py`. Move it to `.env` so prod requires explicit opt-in. Search and replace all references.

---

## Phase 3: Clean Up File Layout
*Goal: project structure that makes sense to a new reader*

1. ~~Rename `backend/` → `weatherbot/`, `frontend/` → `dashboard/`~~ ✓ Done
2. Create `scripts/` directory, move these there:
   - `audit_paper_trades.py`
   - `backfill_paper_trades.py`
   - `calibrate.py`
   - `dry_run.py`
   - `reconstruct_pnl.py`
   - `report.py`
   - `test_discord.py`
   - `test_kalshi_auth.py`
   - `test_weather.py`
3. Add a one-line docstring to each script explaining when to run it
4. Add climatology normals for all 18 cities to `weatherbot/data/weather.py` — or remove the filter entirely. Having it apply to only 5 of 18 cities silently is worse than not having it at all.
5. Rename `weatherbot/models/database.py` → `weatherbot/models/weather_db.py`. Update all imports.

---

## Phase 4: Reliability Fixes
*Goal: the bot doesn't silently lose data on API hiccups*

1. Add retry logic to all external HTTP calls — `httpx.HTTPTransport(retries=2)`. Touch:
   - `weatherbot/data/kalshi_client.py`
   - `weatherbot/data/weather.py`
   - `weatherbot/data/multi_source_weather.py`
2. Convert `weatherbot/notifications/discord.py` from `requests` (sync) to `httpx.AsyncClient` (async) so Discord notifications don't block the event loop
3. Parallelize settlement checks in `weatherbot/core/paper_trading.py` — replace the serial loop with `asyncio.gather()` with a per-trade timeout so one slow Kalshi call doesn't block the whole batch

---

## Phase 5: Add Live Trading
*Goal: real order placement behind a single flag*

1. Add `KALSHI_API_BASE_URL` to `config.py`:
   ```python
   KALSHI_API_BASE_URL: str = "https://demo-api.kalshi.co/trade-api/v2"
   # prod: https://api.kalshi.com/trade-api/v2
   ```
   Replace the hardcoded base URL in `kalshi_client.py` with this setting.

2. Add `place_order()` to `weatherbot/data/kalshi_client.py`:
   ```python
   async def place_order(self, ticker: str, side: str, count: int, yes_price: int) -> dict:
       """side: 'yes' or 'no'. yes_price: cents (e.g. 65 = $0.65). count: number of contracts."""
       return await self._post("/portfolio/orders", {
           "ticker": ticker,
           "action": "buy",
           "side": side,
           "count": count,
           "type": "limit",
           "yes_price": yes_price,
       })
   ```

3. Create `weatherbot/core/order_executor.py`:
   ```python
   async def execute_signal(signal: WeatherTradingSignal) -> None:
       if not settings.LIVE_TRADING:
           await log_paper_trade(signal)
           return

       logger.info(
           f"LIVE ORDER: {signal.market.market_id} "
           f"{signal.direction.upper()} "
           f"count={int(signal.suggested_size)} "
           f"price={int(signal.market.yes_price * 100)}c"
       )
       result = await kalshi_client.place_order(...)
   ```

4. Add a `LIVE_MAX_TRADE_SIZE` cap in `config.py` (e.g. `1.0` = $1 during initial live testing). Override `suggested_size` in `order_executor.py` when `LIVE_TRADING=True`.

5. Create `weatherbot/core/live_trading.py` with `log_live_trade()` — records real orders to a separate DB table so live and paper trades never mix.

6. Update the scheduler to call `execute_signal()` instead of `log_paper_trade()` directly.

7. Add a Discord notification specifically for live order placement and fills.

8. Add a safety assertion in `order_executor.py`:
   ```python
   if settings.LIVE_TRADING:
       assert "demo" not in settings.KALSHI_API_BASE_URL, \
           "LIVE_TRADING=true but pointing at demo API — check your .env"
   ```

9. In `.env` on prod only: `LIVE_TRADING=true`, `KALSHI_API_BASE_URL=https://api.kalshi.com/trade-api/v2`

---

## Testing Live Order Placement (Before Flipping LIVE_TRADING)

Create a demo account at `demo.kalshi.com`, add demo credentials to `.env`, then run `scripts/test_order_placement.py`:

```python
"""
Tests order placement end-to-end against the Kalshi demo environment.
Requires KALSHI_API_BASE_URL=https://demo-api.kalshi.co/trade-api/v2 in .env
"""
import asyncio
from weatherbot.data.kalshi_client import KalshiClient

async def main():
    client = KalshiClient()

    markets = await client.get("/markets", params={"limit": 5})
    ticker = markets["markets"][0]["ticker"]
    print(f"Using ticker: {ticker}")

    result = await client.place_order(ticker=ticker, side="yes", count=1, yes_price=1)
    print(f"Order placed: {result}")

    order_id = result["order"]["id"]
    cancelled = await client.delete(f"/portfolio/orders/{order_id}")
    print(f"Order cancelled: {cancelled}")

asyncio.run(main())
```

---

## Priority Order

1. **Phase 1 first** — pure deletion, zero risk, makes everything else easier
2. **Phase 2** — config consolidation, establishes clean foundation
3. **Phase 3** — file layout cleanup, can happen alongside Phase 2
4. **Phase 4** — reliability fixes, do before going live
5. **Phase 5** — live trading, only after Phases 1-4 are done
