"""Paper trading engine — log signals, settle via NWS observed temps."""
import json
import logging
from datetime import date, datetime, timedelta, timezone
from typing import List, Optional

from backend.config import settings
from backend.models.paper_trade import PaperSessionLocal, PaperTrade, ModelCityAccuracy, init_paper_db

# ── DB init guard ─────────────────────────────────────────────────────────────
_db_initialized = False


def _ensure_db():
    global _db_initialized
    if not _db_initialized:
        init_paper_db()
        _db_initialized = True


# ── Logging a new paper trade ─────────────────────────────────────────────────

def log_paper_trade(signal) -> Optional[PaperTrade]:
    """
    Persist a paper trade from an actionable WeatherTradingSignal.
    Returns the PaperTrade row, or None if it was already logged this session.
    """
    _ensure_db()

    market = signal.market
    entry_price = market.yes_price if signal.direction == "yes" else market.no_price
    if entry_price <= 0:
        return None

    contracts = max(1, int(signal.suggested_size / entry_price))

    db = PaperSessionLocal()
    try:
        # Deduplicate: skip if any unresolved trade already exists for this ticker
        existing = db.query(PaperTrade).filter(
            PaperTrade.ticker == market.market_id,
            PaperTrade.resolved == False,
        ).first()
        if existing:
            logger.debug(f"Duplicate signal skipped: {market.market_id} (already pending)")
            return None

        pt = PaperTrade(
            ticker           = market.market_id,
            city             = market.city_key,
            metric           = market.metric,
            threshold_f      = market.threshold_f,
            side             = signal.direction,
            market_direction = market.direction,
            agreement        = getattr(signal, "agreement", "MEDIUM"),
            model_probs      = json.dumps(getattr(signal, "source_probs", {})),
            model_prob       = signal.model_probability,
            market_price     = signal.market_probability,
            edge             = signal.edge,
            confidence       = signal.confidence,
            kelly_size       = signal.suggested_size,
            contracts        = contracts,
            entry_price      = entry_price,
            forecast_mean    = signal.ensemble_mean,
            forecast_std     = signal.ensemble_std,
            resolution_date  = market.target_date.isoformat(),
            resolved         = False,
        )
        db.add(pt)
        db.commit()
        db.refresh(pt)
        logger.info(
            f"📝 PAPER TRADE logged: {market.market_id}  {signal.direction.upper()}  "
            f"{contracts} contracts @ {entry_price:.2%}  edge={signal.edge:+.1%}"
        )
        return pt
    except Exception as e:
        logger.warning(f"Failed to log paper trade for {market.market_id}: {e}")
        db.rollback()
        return None
    finally:
        db.close()


# ── NWS temperature lookup ────────────────────────────────────────────────────

async def _fetch_nws_daily_temp(city_key: str, target_date: date, metric: str) -> Optional[float]:
    """
    Fetch observed daily HIGH or LOW temperature (°F) from NWS.
    Delegates to weather.py's fetch_nws_observed_temperature which uses the
    exact station coordinates Kalshi uses for settlement.
    """
    from backend.data.weather import fetch_nws_observed_temperature
    obs = await fetch_nws_observed_temperature(city_key, target_date)
    if obs is None:
        return None
    return obs.get(metric)


# ── Settlement ────────────────────────────────────────────────────────────────

async def settle_paper_trades() -> List[PaperTrade]:
    """
    Find unresolved paper trades whose resolution_date has passed,
    fetch the actual NWS temperature, and mark WIN/LOSS.
    Returns list of newly settled trades.
    """
    _ensure_db()
    today = date.today()
    settled = []

    db = PaperSessionLocal()
    try:
        pending = db.query(PaperTrade).filter(
            PaperTrade.resolved == False,
            PaperTrade.resolution_date < today.isoformat(),
        ).all()

        if not pending:
            logger.debug("No paper trades ready for settlement")
            return []

        logger.info(f"Settling {len(pending)} paper trade(s)...")

        for pt in pending:
            target_date = date.fromisoformat(pt.resolution_date)
            actual_temp = await _fetch_nws_daily_temp(pt.city, target_date, pt.metric)

            if actual_temp is None:
                logger.info(f"Skipping settlement for {pt.ticker} — NWS data not available yet")
                continue

            # Determine if the YES condition resolved true
            # market_direction is "above" or "below" (the YES condition)
            if pt.market_direction == "above":
                yes_wins = actual_temp > pt.threshold_f
            else:
                yes_wins = actual_temp < pt.threshold_f

            # We bet "yes" or "no" — did we win?
            if pt.side == "yes":
                we_win = yes_wins
            else:
                we_win = not yes_wins

            if we_win:
                pnl = (1.0 - pt.entry_price) * pt.contracts * (1.0 - settings.KALSHI_FEE_RATE)
                result = "win"
            else:
                pnl = pt.entry_price * pt.contracts * -1.0
                result = "loss"

            pt.actual_temp   = actual_temp
            pt.resolved      = True
            pt.result        = result
            pt.pnl           = round(pnl, 2)
            pt.resolved_at   = datetime.utcnow()
            settled.append(pt)

            # YES resolved = 1.0 if YES condition was true, else 0.0
            if pt.market_direction == "above":
                yes_outcome = 1.0 if actual_temp > pt.threshold_f else 0.0
            else:
                yes_outcome = 1.0 if actual_temp < pt.threshold_f else 0.0

            # Update per-model city accuracy
            try:
                model_probs: dict = json.loads(pt.model_probs or "{}")
                for model_name, model_p in model_probs.items():
                    row = db.query(ModelCityAccuracy).filter(
                        ModelCityAccuracy.model  == model_name,
                        ModelCityAccuracy.city   == pt.city,
                        ModelCityAccuracy.metric == pt.metric,
                    ).first()
                    if not row:
                        row = ModelCityAccuracy(
                            model=model_name, city=pt.city, metric=pt.metric,
                            n=0, brier_sum=0.0, wins=0, losses=0,
                        )
                        db.add(row)
                    row.n         += 1
                    row.brier_sum += (model_p - yes_outcome) ** 2
                    if (model_p > 0.5) == (yes_outcome == 1.0):
                        row.wins += 1
                    else:
                        row.losses += 1
                    row.updated_at = datetime.utcnow()
            except Exception as e:
                logger.debug(f"Failed to update model accuracy for {pt.ticker}: {e}")

            icon = "✅" if result == "win" else "❌"
            logger.info(
                f"{icon} PAPER SETTLED: {pt.ticker}  {result.upper()}  "
                f"actual={actual_temp:.1f}°F  threshold={pt.threshold_f:.0f}°F  "
                f"P&L=${pnl:+.2f}"
            )

        db.commit()

    except Exception as e:
        logger.error(f"Paper trade settlement error: {e}", exc_info=True)
        db.rollback()
    finally:
        db.close()

    return settled


# ── Stats helper (used by report.py and daily summary) ───────────────────────

def get_model_accuracy(db=None) -> list:
    """Return per-model, per-city accuracy rows sorted by Brier score."""
    _ensure_db()
    close_after = db is None
    if db is None:
        db = PaperSessionLocal()
    try:
        rows = db.query(ModelCityAccuracy).all()
        result = []
        for r in rows:
            brier = r.brier_sum / r.n if r.n > 0 else None
            result.append({
                "model": r.model, "city": r.city, "metric": r.metric,
                "n": r.n, "brier": brier,
                "wins": r.wins, "losses": r.losses,
            })
        return sorted(result, key=lambda x: (x["model"], x["city"]))
    finally:
        if close_after:
            db.close()


def get_paper_stats(db=None):
    """Return a dict of aggregate paper trading statistics."""
    _ensure_db()
    close_after = db is None
    if db is None:
        db = PaperSessionLocal()

    try:
        all_trades  = db.query(PaperTrade).all()
        resolved    = [t for t in all_trades if t.resolved]
        unresolved  = [t for t in all_trades if not t.resolved]
        wins        = [t for t in resolved if t.result == "win"]
        losses      = [t for t in resolved if t.result == "loss"]
        total_pnl   = sum(t.pnl for t in resolved if t.pnl is not None)
        avg_edge    = (sum(t.edge for t in all_trades) / len(all_trades)) if all_trades else 0.0

        # Brier score: mean((model_prob - actual_outcome)^2)
        # actual_outcome = 1 if YES won, 0 if NO won
        brier_scores = []
        for t in resolved:
            if t.market_direction == "above":
                yes_won = 1.0 if t.actual_temp > t.threshold_f else 0.0
            else:
                yes_won = 1.0 if t.actual_temp < t.threshold_f else 0.0
            brier_scores.append((t.model_prob - yes_won) ** 2)
        brier = (sum(brier_scores) / len(brier_scores)) if brier_scores else None

        # City breakdown
        cities = {}
        for t in resolved:
            c = cities.setdefault(t.city, {"wins": 0, "losses": 0, "pnl": 0.0})
            if t.result == "win":
                c["wins"] += 1
            else:
                c["losses"] += 1
            c["pnl"] += t.pnl or 0.0

        # Agreement breakdown
        agreement_levels = {}
        for t in resolved:
            lvl = getattr(t, "agreement", "MEDIUM") or "MEDIUM"
            a = agreement_levels.setdefault(lvl, {"wins": 0, "losses": 0, "pnl": 0.0})
            if t.result == "win":
                a["wins"] += 1
            else:
                a["losses"] += 1
            a["pnl"] += t.pnl or 0.0

        return {
            "total":      len(all_trades),
            "resolved":   len(resolved),
            "unresolved": len(unresolved),
            "wins":       len(wins),
            "losses":     len(losses),
            "total_pnl":  total_pnl,
            "avg_edge":   avg_edge,
            "brier":      brier,
            "cities":     cities,
            "agreement_levels": agreement_levels,
            "all_trades": all_trades,
            "resolved_trades": resolved,
        }
    finally:
        if close_after:
            db.close()
