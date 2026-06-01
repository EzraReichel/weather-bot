# Weather Bot — Going Live & Code Review

This document has **two independent sections**:

1. **[Going Live](#section-1--going-live-checklist)** — the operational checklist for safely
   turning live trading on/off and deploying.
2. **[Code Review](#section-2--code-review)** — a full correctness/logic review of the
   trading pipeline.

---

# SECTION 1 — Going Live Checklist

A repeatable procedure for putting real money behind the bot. Treat every box as
blocking: if you can't tick it, don't go live.

## Pre-flight (before flipping `LIVE_TRADING=true`)

- [ ] **Confirm the deploy is current.** `git log --oneline -1` locally matches the
      commit Render actually built. A push triggers a Render redeploy; verify the
      build finished and the new commit hash is live before trusting any fix.
- [ ] **Settlement runs on boot.** `start_scheduler()` fires `settlement_job()`
      immediately on startup (`scheduler.py`). Any redeploy = an immediate settlement
      sweep. Make sure the DB is in a state where that's safe (no half-reconciled rows).
- [ ] **Reconcile open positions in the DB vs. Kalshi.** For every `resolved=False`
      live row, confirm the recorded `contracts` matches your true Kalshi position.
      Manually-cancelled orders, partial fills, or a runaway can leave the DB row
      disagreeing with reality — and settlement trusts the DB row's `entry_price`.
- [ ] **Check `LIVE_MAX_TRADE_SIZE`.** This is the hard per-position dollar cap. Start
      small (e.g. `$5`) until you've watched real fills for a few days. Confirm the
      value in Render env, not just `.env.example`.
- [ ] **Confirm `KALSHI_API_BASE_URL` is production, not demo**, and that
      `KALSHI_API_KEY_ID` + the private key are set. `execute_signal()` asserts on
      this, but check before, not after.
- [ ] **Sanity-check the bankroll source.** The bot sizes off the live Kalshi balance
      (`scan_for_weather_signals`). Confirm the account balance is what you expect —
      a wrong balance silently rescales every bet.

## Turning it on

- [ ] Set `LIVE_TRADING=true` in Render env.
- [ ] Watch the first scan's logs: confirm orders are placed at sane prices, sane
      sizes, and on **weather tickers only** (`KXHIGH/KXLOW/KXRAIN`).
- [ ] Watch the first **top-up** and the first **settlement** before walking away.
      Those are the two paths that have broken before.

## Kill switch (do this the moment something looks wrong)

- [ ] **Set `LIVE_TRADING=false`** in Render (or pause the service). The scheduler keeps
      running but `execute_signal()` routes to paper instead of placing real orders.
- [ ] **Cancel resting orders on Kalshi manually** if a position is mid-build. The bot
      never cancels orders itself (`cancel_order` exists but is never called).
- [ ] After any incident, **reconcile the DB** before re-enabling (see pre-flight).

## Known sharp edges (live)

- **30-second scan interval** (`SCAN_INTERVAL_SECONDS`). Anything that re-orders or
  loops escalates in *minutes*, not hours. If a bug slips through, the blast radius is
  large at this cadence. Consider raising to 5–15 min.
- **No retry on unfilled orders by design.** The cumulative-ordered cap means an order
  that partially fills or gets cancelled is NOT re-tried. The position sits at whatever
  filled. Safe, but it means missed size is missed permanently until the next day.
- **Settlement trusts the DB row.** If `contracts`/`entry_price` on a row are wrong,
  P&L will be wrong. Reconciliation is the only guard.

---

# SECTION 2 — Code Review

> **Original review prompt (verbatim):**
>
> I want to do an entire code review here, checking for both correctness and logic.
> Start with the scheduler logic and ensure we are running checks for new bets to place
> and checks for current bets correctly, efficiently, and in a way that maximizes
> profits. next, check the weather data logic. check we are getting the highest quality
> data at the highest possible frequency and check we are combining data correctly.
> remember that kalshi resolves bets using data from city's airports. next, check the
> bet placement and sizing logic, both that it works as intended and we are using the
> best possible methods to determine sizing. finally, check settling each bet, both
> correctness and profitability when orders are partially filled, completely filled, or
> not filled at all. you are a senior developer code reviewing an intern's code. dont
> sugar coat anything, be blunt but correct, and ask questions yourselves. deliver a
> complete report.

**Reviewer's note:** Three bugs from the first pass have since been **fixed** (NO-side
order pricing, the runaway re-buy loop, and UTC-vs-ET settlement timing) — they're
marked ✅ FIXED below for context but kept in the record. Everything else stands.

## 0. Strategic finding — you only trade the extremes, never the range

**This is the highest-value finding in the review, so it goes first.**

The bot **explicitly discards every "between X and Y" bracket market.** In
`kalshi_markets.py`:

```python
if boundary_type == "B":
    return None   # bracket range — not binary      (line 174)
...
if "-B" in ticker:
    report.filtered.append(... reason="bracket")     (line 274)
    continue
```

So you only ever trade binary threshold markets (`-T`): "high above 80", "low below 72".
You're right that a NO on "below 72" is economically *similar* to betting the temp lands
in some upper range — but it is **strictly worse**, for two concrete reasons:

1. **Lower payout / worse pricing.** A binary threshold deep in the tail (e.g. "above 80"
   when you think it'll be 75) prices near the extreme, so your edge per dollar is thin
   and the entry-price floors (`YES_ENTRY_FLOOR = 0.30`) filter most of them out anyway.
   A **range bracket centered on your forecast** ("between 74 and 76") is priced on the
   *mode* of the distribution, where your ensemble is most confident and the market is
   often mispriced — that's where the fat edge lives.

2. **Your model already produces exactly what range markets need.** You compute a full
   ensemble distribution (`member_highs`, mean, std). A bracket's probability is just
   `P(low ≤ T < high)` = `CDF(high) − CDF(low)` — trivially computable from the same
   members you already have. You're throwing away the part of your model that's most
   informative (the peak) and only betting the tails (where you're least sure and the
   market is most efficient).

**Questions for you:**
- What's the actual volume/liquidity on Kalshi's bracket markets per city? If it's
  thin, the edge may be unrealizable — worth measuring before building.
- Bracket markets are mutually exclusive within a day. Sizing across N correlated
  brackets is a different (and harder) problem than the current single-threshold Kelly.
  Are you prepared to handle the correlation, or would you trade one bracket per city/day?

**Recommendation:** This is a feature, not a bug, but it's likely leaving the best edge
on the table. Prototype it: parse `-B` tickers, compute `CDF(high) − CDF(low)` from the
existing ensemble, and **paper trade it alongside** the threshold strategy for a few
weeks before risking capital. Do not bolt it onto live trading directly.

## 1. Scheduler logic

### 🔴 You scan every 30s but the data updates ~4×/day, uncached
`SCAN_INTERVAL_SECONDS = 30` drives `weather_scan_job`. Each scan calls
`fetch_all_sources` per market — 4 uncached HTTP calls (GFS/ECMWF/GEM/NWS). The
`_forecast_cache` (15-min TTL) exists **only on the single-source fallback path** you
rarely hit; the multi-source path has **zero caching**. Result: tens of thousands of
forecast API calls per hour to recompute forecasts that change 4×/day. Open-Meteo /
api.weather.gov will throttle or ban you, and when they do, sources silently drop and
your signal degrades.

This also *amplified the runaway*: a re-buy bug at 30s cadence piles up in minutes.

**Fix:** cache `fetch_all_sources` keyed `(city_key, target_date)` with a 1–4h TTL, and
raise the scan interval to 5–15 min. The 4 cron "model-run" scans are redundant with a
short interval scan — pick one strategy.

### 🔴 You fetch weather for markets you immediately discard
`_dedup_correlated` keeps one signal per `(city, date, metric)` — but runs *after*
`generate_weather_signal` (4 fetches) has already run for **every threshold market**.
NYC high @ 70/71/72/73°F each re-fetch the identical sources, then all but one are
dropped. **Fix:** dedup to one market per `(city,date,metric)` before the fetch loop, or
let the cache above absorb it.

### 🟡 Minor
- `_alerted_tickers` grows unbounded — never evicted. Slow memory leak. Prune entries
  older than the dedup window.
- Settlement runs hourly but markets resolve once/day ET. Harmless now; revisit at scale.

## 2. Weather data quality

### 🔴 The NWS "ensemble" is fabricated, and it carries 30% weight
`_fetch_nws_point_forecast` takes a **single** NWS forecast number and manufactures a
21-member "ensemble" via Gaussian noise (`rng.normal(day_high, sigma, 21)`). Problems:
1. The `ensemble_fraction` signal (70% of the blend) becomes pure sampling noise for
   NWS — it adds nothing beyond `day_high ± sigma`.
2. `sigma` is a fixed ±3/±5°F regardless of city, season, or lead time.
3. The RNG seed is **date-only**, so every city draws the identical noise shape — NWS
   "members" are not independent across cities.

NWS gets the **joint-highest** static weight (0.30) precisely because it's closest to
the resolution source — and you're feeding that weight synthetic statistics. **Fix:**
use the NWS point value as a single deterministic input (no fake spread), or pull a real
probabilistic product (NBM percentiles).

### 🟡 Grid forecast vs. station thermometer — no bias correction
Kalshi resolves on a specific station. Open-Meteo returns the nearest **grid cell**,
which ≠ the station thermometer (airport siting runs hot/cold). No station-level bias
correction exists. The per-city Brier weighting partially absorbs this into weights but
won't fix a systematic mean offset — exactly the 1–2°F that flips a binary threshold.

**On the "airports" premise:** mostly true (KORD, KDEN, KLAX, KMIA, KAUS are airports),
but **NYC resolves on Central Park (KNYC), not an airport.** The config gets this right;
just don't bake an "always the airport" assumption anywhere.

### 🟡 Dead settlement code
`fetch_nws_observed_temperature` computes observed high/low for settlement, but
settlement uses Kalshi's official `result` instead (correct — it's authoritative). The
NWS-observed function is dead weight implying a path you don't use. Delete it or document
it as a fallback.

### 🟢 Good
- Parallel fetch with graceful `ok=False` degradation.
- One API call for both max and min.
- Per-city/metric inverse-Brier dynamic weighting — genuinely nice, self-correcting.

## 3. Bet placement & sizing

### ✅ FIXED — NO orders priced off the wrong side (never filled)
`place_order` only sent `yes_price`; a NO buy was implied at `100 − yes_ask`, a full
spread below the real NO ask, so it never crossed and Kalshi cancelled it. **Now** sends
`no_price` for NO orders. This was the root cause of the "cancelled" trades.

### ✅ FIXED — Runaway re-buy loop
The top-up cap measured live `filled + resting`; when Kalshi cancelled a resting order
the count collapsed to ~0 and the bot re-ordered the full target every scan (blew one
position to 395 contracts / $209 across 13 orders). **Now** caps on cumulative contracts
ordered from the DB. Cancelled/unfilled orders cannot refund the budget.

### 🟡 The Kalshi fee model is wrong
`KALSHI_FEE_RATE = 0.07` is applied as "7% of profit": `pnl = (1-entry) * n * (1-0.07)`
and `min_profitable_edge = fee/(1-fee)`. Kalshi's actual trading fee is approximately
`roundup(0.07 × n × price × (1−price))`, charged at execution on **both winners and
losers**. Consequences:
- Near the money you **overstate fees ~2.5×** → demand too much edge → skip profitable
  trades.
- You only deduct fees from winners (the loss branch has no fee) → losses understated,
  backtest P&L optimistic.

**Fix:** implement the real `C×P×(1−P)` formula in one place, used by both the edge gate
and settlement. (Verify against Kalshi's current schedule before coding — it changes.)

### 🟡 Kelly bankroll uses available cash, not equity
`_available_bankroll` = cash − committed `kelly_size`, then all signals are scaled
uniformly. Two issues: (1) Kelly should size off total equity (cash + open-position
value), so as the book fills you progressively **under-bet** vs. true Kelly; (2) uniform
scaling shrinks a 20%-edge and a 9%-edge bet by the same ratio — not optimal allocation
across competing edges. Acceptable v1 guardrail; know it's not optimal.

### 🟡 Floor/ceiling sizing substitution is a calibration band-aid
When `model_prob` clamps to 0.05/0.95 you sub 0.15/0.85 for sizing — the comment admits
these "95%" trades win ~45%. That's a **calibration failure** patched at the sizing
layer with magic constants. The right fix is to calibrate the probability model
(isotonic/Platt on settled trades) so 0.95 means 0.95.

### 🟢 Good
- Fractional Kelly (0.15) — appropriately conservative.
- `max(1, int(size/price))` with the undersize-skip guard prevents phantom trades.
- Anti-chase / averaging-down top-up guards are sound.

## 4. Settlement (partial / full / no fill)

### ✅ FIXED — UTC vs ET settlement timing
`date.today()` on Render (UTC) rolls over ~7–8 PM ET, pulling next-day-ET markets into
settlement ~a day early. **Now** uses `et_today()`.

### 🟡 Undeterminable order at settlement can book a phantom loss
When `get_order` can't be fetched at settlement (`undetermined > 0`) with zero confirmed
fills, the code falls back to the recorded contract count and settles as if fully filled.
If that order never actually filled, you book a **loss on contracts you never held**. The
"assume filled" fallback is correct defensively for *sizing* (don't double-order) but
wrong at *settlement* — there it should refuse to realize P&L on unconfirmable fills and
retry next cycle.

### 🟡 Resting remainder can block settlement forever
`if total_resting > 0: continue` skips until resolved. An order that rests through expiry
without formally cancelling leaves the position unsettled forever, holding committed
capital in `_available_bankroll` and starving sizing. **Fix:** once `resolution_date` is
N hours past, treat resting as cancelled and settle on confirmed fills.

### 🟢 Good
- Recomputing blended basis from **actual** fills at settlement — exact, verified.
- Using Kalshi's official `result` as settlement truth — correct and authoritative.

## Priority order if you fix nothing else

| # | Sev | Issue |
|---|-----|-------|
| 1 | 🔴 | Cache `fetch_all_sources` + raise scan interval — stop hammering APIs |
| 2 | 🔴 | Dedup markets *before* fetching weather |
| 3 | 🔴 | Stop fabricating the NWS ensemble |
| 4 | 🟢→🟡 | Evaluate range/bracket markets (Section 0) — likely biggest edge upside |
| 5 | 🟡 | Implement Kalshi's real `C×P×(1−P)` fee in one place |
| 6 | 🟡 | Station-vs-grid bias correction |
| 7 | 🟡 | Settlement: don't realize P&L on unverifiable fills; resting-order expiry backstop |

\#1 and \#2 are an afternoon's work and cut API load ~100×. \#3 is most likely silently
costing money on close thresholds. \#4 is the strategic upside worth prototyping in paper.
