"""
Calibration harness over the bt_* archive (read-only).

Measures the *probabilistic forecast quality* of the model — Brier score and
reliability — independent of the trading filters. The point is to answer "when
the model says 70%, does it happen 70% of the time?" and to test whether a
per-(city, metric) bias correction and the calibration knobs (std floors, the
ensemble/CDF blend weight) improve that.

Design constraints, given we only have ~15 days of single-season data:
  • Score by **cross-validated Brier**, never by backtest P&L (15 summer days
    of P&L just rediscovers "don't trade in summer").
  • Bias correction is estimated by **leave-one-day-out** so a day is never
    scored with a bias that saw its own outcome.
  • The sweep covers only a few low-degrees-of-freedom continuous knobs on a
    coarse grid — anything broader overfits this little data.

Nothing here touches live trading or writes to the DB.
"""
from __future__ import annotations

import json
import logging
import statistics
from dataclasses import dataclass, field
from datetime import date, datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple

from sqlalchemy import text

from weatherbot.core.probability import compute_multi_source_probability, SOURCE_WEIGHTS
from weatherbot.core.strategy import StrategyParams
from weatherbot.data.multi_source_weather import SourceForecast

logger = logging.getLogger("weatherbot")

ET_RESOLUTION_HOUR = 23  # Kalshi temp markets settle at end of target_date ET


@dataclass
class ForecastPoint:
    """One captured multi-source forecast for a (city, target_date)."""
    captured_at: datetime           # naive UTC
    members: Dict[str, List[float]] = field(default_factory=dict)  # source -> member temps (chosen metric)


@dataclass
class CalEvent:
    """A settled market plus the forecast we score it with."""
    ticker: str
    city_key: str
    metric: str               # high | low
    direction: str            # above | below
    threshold_f: float
    target_date: date
    observed_f: float         # expiration_value — the actual high/low
    outcome: int              # 1 if YES resolved, else 0
    forecast: ForecastPoint
    lead_hours: float         # captured_at → resolution, for reporting


# ── Loading ───────────────────────────────────────────────────────────────────

def _resolution_utc(target_date: date) -> datetime:
    from zoneinfo import ZoneInfo
    et = ZoneInfo("America/New_York")
    return datetime(target_date.year, target_date.month, target_date.day,
                    ET_RESOLUTION_HOUR, 59, 0, tzinfo=et).astimezone(timezone.utc).replace(tzinfo=None)


def _members_for_metric(sources_json, metric: str) -> Dict[str, List[float]]:
    src = sources_json if isinstance(sources_json, dict) else json.loads(sources_json or "{}")
    key = "member_highs" if metric == "high" else "member_lows"
    out: Dict[str, List[float]] = {}
    for name, sd in src.items():
        if not sd.get("ok"):
            continue
        vals = sd.get(key) or []
        if vals:
            out[name] = [float(v) for v in vals]
    return out


def load_events(
    db,
    start: Optional[str] = None,
    end: Optional[str] = None,
    metrics: Tuple[str, ...] = ("high", "low"),
    lead_hours_target: float = 24.0,
) -> List[CalEvent]:
    """
    Build one CalEvent per settled temperature market that has a usable forecast.

    For each market we pick the forecast snapshot whose capture time is closest
    to ``lead_hours_target`` before resolution (a consistent ~day-ahead decision
    point), among snapshots captured at or before resolution. Falls back to the
    earliest snapshot when nothing that early exists (capture began mid-window).
    """
    # ── Settlements (ground truth) ────────────────────────────────────────────
    where = ["result IN ('yes','no')", "expiration_value IS NOT NULL", "metric = ANY(:metrics)"]
    p: dict = {"metrics": list(metrics)}
    if start:
        where.append("target_date >= :start"); p["start"] = start
    if end:
        where.append("target_date <= :end"); p["end"] = end
    srows = db.execute(text(
        f"SELECT ticker, city_key, metric, direction, threshold_f, target_date, "
        f"result, expiration_value FROM bt_settlements WHERE {' AND '.join(where)}"
    ), p).mappings().all()

    # ── Weather snapshots, grouped by (city, target_date) ─────────────────────
    wrows = db.execute(text(
        "SELECT city_key, target_date, captured_at, sources FROM bt_weather_snapshots "
        "ORDER BY captured_at ASC"
    )).mappings().all()
    by_key: Dict[Tuple[str, str], list] = {}
    for w in wrows:
        by_key.setdefault((w["city_key"], w["target_date"]), []).append(w)

    events: List[CalEvent] = []
    for s in srows:
        tdate = date.fromisoformat(s["target_date"])
        res_utc = _resolution_utc(tdate)
        target_utc = res_utc - timedelta(hours=lead_hours_target)
        snaps = by_key.get((s["city_key"], s["target_date"]))
        if not snaps:
            continue
        # snapshots captured before resolution; pick the one closest to target lead
        usable = [w for w in snaps if w["captured_at"] <= res_utc]
        if not usable:
            continue
        chosen = min(usable, key=lambda w: abs((w["captured_at"] - target_utc).total_seconds()))
        members = _members_for_metric(chosen["sources"], s["metric"])
        if not members:
            continue
        events.append(CalEvent(
            ticker=s["ticker"], city_key=s["city_key"], metric=s["metric"],
            direction=s["direction"], threshold_f=float(s["threshold_f"]),
            target_date=tdate, observed_f=float(s["expiration_value"]),
            outcome=1 if s["result"] == "yes" else 0,
            forecast=ForecastPoint(captured_at=chosen["captured_at"], members=members),
            lead_hours=(res_utc - chosen["captured_at"]).total_seconds() / 3600.0,
        ))
    return events


# ── Bias estimation ─────────────────────────────────────────────────────────

def _weighted_center(members: Dict[str, List[float]], weights: Dict[str, float]) -> Optional[float]:
    """Weighted mean of per-source ensemble means — the center the model sits on."""
    num = den = 0.0
    for name, vals in members.items():
        if not vals:
            continue
        w = weights.get(name, 0.0)
        if w <= 0:
            continue
        num += w * statistics.mean(vals)
        den += w
    if den <= 0:
        return None
    return num / den


def estimate_bias(
    events: List[CalEvent],
    weights: Dict[str, float],
    min_samples: int = 5,
) -> Dict[Tuple[str, str], float]:
    """
    Per-(city, metric) additive bias = mean(observed − forecast_center).

    Positive bias means the grid forecast runs COLD vs the station and members
    should be shifted up. Returns {} entries only where >= ``min_samples`` days
    exist; callers treat a missing key as bias 0.
    """
    resid: Dict[Tuple[str, str], List[float]] = {}
    for e in events:
        center = _weighted_center(e.forecast.members, weights)
        if center is None:
            continue
        resid.setdefault((e.city_key, e.metric), []).append(e.observed_f - center)
    return {k: statistics.mean(v) for k, v in resid.items() if len(v) >= min_samples}


# ── Scoring ───────────────────────────────────────────────────────────────────

def _model_prob(e: CalEvent, params: StrategyParams, bias: float) -> Optional[float]:
    """P(YES) for an event under ``params`` with members shifted by ``bias``."""
    sources: Dict[str, SourceForecast] = {}
    for name, vals in e.forecast.members.items():
        shifted = [v + bias for v in vals] if bias else list(vals)
        sources[name] = SourceForecast(source=name, member_highs=shifted,
                                       member_lows=shifted, ok=True)
    as_of = e.forecast.captured_at.replace(tzinfo=timezone.utc)
    res = compute_multi_source_probability(
        sources=sources, threshold_f=e.threshold_f, direction=e.direction,
        target_date=e.target_date, metric=e.metric, city_key="",  # static weights, no DB
        params=params, as_of=as_of,
    )
    return res.combined_prob if res else None


def brier(preds: List[Tuple[float, int]]) -> float:
    return sum((p - o) ** 2 for p, o in preds) / len(preds) if preds else float("nan")


def reliability_curve(preds: List[Tuple[float, int]], n_buckets: int = 10) -> List[dict]:
    from collections import defaultdict
    buckets: Dict[int, list] = defaultdict(list)
    for p, o in preds:
        buckets[min(int(p * n_buckets), n_buckets - 1)].append((p, o))
    out = []
    for i in range(n_buckets):
        items = buckets.get(i)
        if not items:
            continue
        out.append({
            "bucket": f"{i/n_buckets:.1f}-{(i+1)/n_buckets:.1f}",
            "model_mean": round(sum(p for p, _ in items) / len(items), 3),
            "actual_freq": round(sum(o for _, o in items) / len(items), 3),
            "n": len(items),
        })
    return out


def lodo_cv_predictions(
    events: List[CalEvent],
    params: StrategyParams,
    use_bias: bool,
    min_samples: int = 5,
) -> List[Tuple[float, int]]:
    """
    Leave-one-day-out predictions. For each target_date, the bias (if enabled)
    is estimated from all OTHER days, then applied to score that day's events —
    so no event is ever scored with a bias that saw its own outcome.
    """
    weights = params.source_weights or SOURCE_WEIGHTS
    days = sorted({e.target_date for e in events})
    preds: List[Tuple[float, int]] = []
    for held in days:
        train = [e for e in events if e.target_date != held]
        test = [e for e in events if e.target_date == held]
        bias_map = estimate_bias(train, weights, min_samples) if use_bias else {}
        for e in test:
            bias = bias_map.get((e.city_key, e.metric), 0.0) if use_bias else 0.0
            p = _model_prob(e, params, bias)
            if p is not None:
                preds.append((p, e.outcome))
    return preds
