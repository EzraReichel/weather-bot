"""
Backtest data capture — snapshots every scan into the bt_* archive.

Called once per scan (from the scheduler, after trading decisions are made).
Everything here is best-effort: any exception is swallowed and logged so a
capture failure can never disrupt a scan or a live trade.

Volume control (this writes to a paid Render DB on every scan):
  * Market rows are deduped per-ticker in memory — a new row is only written
    when price (yes_ask/yes_bid/no) or volume_24h moves. Thin weather markets
    rarely move every 5 min, so this collapses ~288 scans/day into a handful
    of rows per ticker while still recording the full price path.
  * Weather rows are deduped by content hash — members only change on a new
    model run (~4-6/day), so identical forecasts are written once.
"""
import hashlib
import json
import logging
import statistics
import uuid
from datetime import date, datetime
from typing import Dict, List, Optional, Tuple

from weatherbot.config import settings
from weatherbot.models.backtest_db import (
    BtMarketSnapshot,
    BtScan,
    BtSignalSnapshot,
    BtWeatherSnapshot,
    init_backtest_db,
)
from weatherbot.models.weather_db import SessionLocal

logger = logging.getLogger("weatherbot")

# Per-ticker last-seen market state for in-process dedup.
# ticker -> (yes_ask, yes_bid, no_ask, volume_24h)
_last_market_state: Dict[str, Tuple] = {}

# Filter reasons worth archiving — these are threshold-tunable, so capturing
# them lets us backtest "what if MIN_VOLUME_24H were lower". Bracket/expired/
# no_price markets aren't binary-backtestable, so we skip them to cut noise.
_ARCHIVABLE_FILTER_REASONS = {
    "near_certain", "low_ask", "low_volume", "stale_50cent", "wide_spread",
}

_tables_ready = False


def _ensure_tables():
    global _tables_ready
    if not _tables_ready:
        init_backtest_db()
        _tables_ready = True


def _days_to_resolution(target_date: date) -> Optional[int]:
    try:
        return (target_date - date.today()).days
    except Exception:
        return None


def _market_changed(ticker: str, key: Tuple) -> bool:
    """True if this ticker is new or its price/volume moved since last capture."""
    prev = _last_market_state.get(ticker)
    if prev != key:
        _last_market_state[ticker] = key
        return True
    return False


def _source_summary(member_highs: List[float], member_lows: List[float], ok: bool) -> dict:
    """Build the per-source JSON blob with raw members + derived stats."""
    def _stats(vals: List[float]) -> Tuple[Optional[float], Optional[float]]:
        if not vals:
            return None, None
        mean = round(statistics.mean(vals), 2)
        std = round(statistics.pstdev(vals), 2) if len(vals) > 1 else 0.0
        return mean, std

    mean_high, std_high = _stats(member_highs)
    mean_low, std_low = _stats(member_lows)
    return {
        "member_highs": [round(v, 2) for v in member_highs],
        "member_lows":  [round(v, 2) for v in member_lows],
        "mean_high": mean_high, "std_high": std_high,
        "mean_low":  mean_low,  "std_low":  std_low,
        "n": len(member_highs) or len(member_lows),
        "ok": bool(ok),
    }


def _content_hash(city_key: str, target_date: str, sources: dict) -> str:
    """Stable hash of the forecast content for dedup."""
    payload = json.dumps(
        {"c": city_key, "d": target_date,
         "s": {k: [v.get("member_highs"), v.get("member_lows")] for k, v in sources.items()}},
        sort_keys=True,
    )
    return hashlib.sha256(payload.encode()).hexdigest()[:32]


async def _capture_weather(db, scan_id: str, city_dates: set) -> int:
    """
    Snapshot per-source ensemble members for each unique (city, target_date).

    Re-calls fetch_all_sources, which hits the 2h in-process cache the scan
    just populated — so this is essentially free (no extra API calls). Returns
    the number of *new* (content-distinct) weather rows written.
    """
    from weatherbot.data.multi_source_weather import fetch_all_sources, fetch_current_observation

    written = 0
    for city_key, target_date in city_dates:
        try:
            raw_sources = await fetch_all_sources(city_key, target_date)
        except Exception as e:
            logger.debug(f"[bt] weather fetch failed {city_key} {target_date}: {e}")
            continue

        sources_json = {
            name: _source_summary(src.member_highs, src.member_lows, src.ok)
            for name, src in raw_sources.items()
        }
        date_str = target_date.isoformat()
        chash = _content_hash(city_key, date_str, sources_json)

        # Skip if we already archived this exact forecast content.
        exists = db.query(BtWeatherSnapshot.id).filter(
            BtWeatherSnapshot.content_hash == chash
        ).first()
        if exists:
            continue

        observation = None
        try:
            observation = await fetch_current_observation(city_key)
        except Exception:
            pass

        db.add(BtWeatherSnapshot(
            scan_id=scan_id,
            city_key=city_key,
            target_date=date_str,
            content_hash=chash,
            sources=sources_json,
            observation=observation,
        ))
        written += 1
    return written


def _capture_markets(db, scan_id: str, fetch_report) -> int:
    """Snapshot tradeable + (archivable) filtered markets, deduped per ticker."""
    n = 0
    # Tradeable markets — full WeatherMarket objects
    for m in getattr(fetch_report, "markets", []):
        key = (m.yes_ask, m.yes_bid, m.no_price,
               getattr(m, "volume_24h", 0.0), getattr(m, "yes_ask_size", 0.0))
        if not _market_changed(m.market_id, ("T",) + key):
            continue
        series = m.market_id.split("-")[0] if "-" in m.market_id else None
        db.add(BtMarketSnapshot(
            scan_id=scan_id,
            ticker=m.market_id,
            series_ticker=series,
            city_key=m.city_key,
            city_name=m.city_name,
            metric=m.metric,
            direction=m.direction,
            threshold_f=m.threshold_f,
            target_date=m.target_date.isoformat(),
            days_to_resolution=_days_to_resolution(m.target_date),
            yes_ask=m.yes_ask or m.yes_price,
            yes_bid=m.yes_bid,
            no_ask=m.no_price,
            volume=getattr(m, "volume", 0.0),
            volume_24h=getattr(m, "volume_24h", 0.0) or None,
            yes_ask_size=getattr(m, "yes_ask_size", 0.0) or None,
            tradeable=True,
            filter_reason=None,
        ))
        n += 1

    # Filtered markets — limited fields (FilteredMarket), only tunable reasons
    for f in getattr(fetch_report, "filtered", []):
        if f.reason not in _ARCHIVABLE_FILTER_REASONS:
            continue
        key = ("F", f.reason, f.yes_price, f.volume_24h, f.ask_size)
        if not _market_changed(f.ticker, key):
            continue
        db.add(BtMarketSnapshot(
            scan_id=scan_id,
            ticker=f.ticker,
            city_key=f.city,
            yes_ask=f.yes_price or None,
            volume_24h=f.volume_24h,
            yes_ask_size=f.ask_size,
            tradeable=False,
            filter_reason=f.reason,
        ))
        n += 1
    return n


def _capture_signals(db, scan_id: str, signals) -> int:
    """Snapshot every generated signal (including filtered, edge=0)."""
    n = 0
    for s in signals:
        m = s.market
        bet_side = s.direction
        entry_price = m.yes_price if bet_side == "yes" else m.no_price
        db.add(BtSignalSnapshot(
            scan_id=scan_id,
            ticker=m.market_id,
            city_key=m.city_key,
            metric=m.metric,
            direction=m.direction,
            threshold_f=m.threshold_f,
            target_date=m.target_date.isoformat(),
            model_probability=s.model_probability,
            market_probability=s.market_probability,
            edge=s.edge,
            bet_side=bet_side,
            confidence=s.confidence,
            kelly_fraction=s.kelly_fraction,
            suggested_size=s.suggested_size,
            entry_price=entry_price,
            ensemble_mean=s.ensemble_mean,
            ensemble_std=s.ensemble_std,
            ensemble_members=s.ensemble_members,
            agreement=s.agreement,
            sources_used=s.sources_used,
            source_probs=s.source_probs,
            low_confidence_flag=s.low_confidence_flag,
            filter_reason=s.filter_reason or None,
            passes_threshold=s.passes_threshold,
            passes_paper_threshold=s.passes_paper_threshold,
            reasoning=s.reasoning,
        ))
        n += 1
    return n


async def capture_scan(scan_report, trigger: str = "interval", bankroll: Optional[float] = None) -> Optional[str]:
    """
    Persist a full point-in-time snapshot of one scan into the bt_* archive.

    Best-effort: returns the scan_id on success, None on any failure (logged).
    Never raises — safe to call from the live scan path.
    """
    if not settings.BACKTEST_CAPTURE:
        return None

    scan_id = uuid.uuid4().hex
    started = datetime.utcnow()

    try:
        _ensure_tables()
    except Exception as e:
        logger.warning(f"[bt] could not init backtest tables: {e}")
        return None

    fetch_report = getattr(scan_report, "fetch_report", None)
    signals = getattr(scan_report, "signals", []) or []

    db = SessionLocal()
    try:
        n_markets = _capture_markets(db, scan_id, fetch_report) if fetch_report else 0
        n_signals = _capture_signals(db, scan_id, signals)

        # Unique (city, date) set drives weather capture
        city_dates = {(m.city_key, m.target_date) for m in getattr(fetch_report, "markets", [])} if fetch_report else set()
        n_weather = await _capture_weather(db, scan_id, city_dates)

        actionable = sum(1 for s in signals if getattr(s, "passes_threshold", False))
        db.add(BtScan(
            scan_id=scan_id,
            started_at=started,
            completed_at=datetime.utcnow(),
            trigger=trigger,
            live_trading=settings.LIVE_TRADING,
            bankroll=bankroll,
            markets_tradeable=len(getattr(fetch_report, "markets", [])) if fetch_report else 0,
            markets_filtered=len(getattr(fetch_report, "filtered", [])) if fetch_report else 0,
            signals_total=len(signals),
            signals_actionable=actionable,
            weather_captured=n_weather,
        ))
        db.commit()
        logger.info(
            f"[bt] captured scan {scan_id[:8]}: {n_markets} market rows, "
            f"{n_signals} signal rows, {n_weather} new weather rows"
        )
        return scan_id
    except Exception as e:
        logger.warning(f"[bt] capture failed: {e}", exc_info=True)
        try:
            db.rollback()
        except Exception:
            pass
        return None
    finally:
        db.close()
