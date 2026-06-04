"""
The shared decision core: ``decide(weather_day, strategy) -> Decision``.

Pure — no I/O, no wall-clock (time comes from ``day.as_of``). It mirrors the
live filter cascade in weatherbot/core/weather_signals.py (divergence →
observation → climatology → trading-hours/conviction → entry-price → edge →
Kelly), but reads everything from the ``WeatherDay`` and the ``StrategyParams``,
and reuses the parameterized probability functions in
weatherbot/core/probability.py for the actual model math (so the highest-risk,
most-tuned layer is genuinely shared, not duplicated).

tests/test_engine_fidelity.py replays captured bt_signal_snapshots through this
function and asserts it reproduces what the live bot recorded — the guardrail
against this core drifting from live behavior.
"""
from __future__ import annotations

import logging
import statistics
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional
from zoneinfo import ZoneInfo

from weatherbot.core.probability import (
    compute_multi_source_probability,
    compute_probability,
    kelly_size,
    min_profitable_edge,
    MultiSourceResult,
)
from weatherbot.core.strategy import StrategyParams
from weatherbot.backtest.world import WeatherDay
from weatherbot.data.multi_source_weather import SourceForecast

logger = logging.getLogger("weatherbot")
ET = ZoneInfo("America/New_York")


@dataclass
class Decision:
    """Output of decide() — a bet, a filtered/edge-0 signal, or (caller maps None)."""
    market: object                       # WeatherMarket
    model_probability: float = 0.5
    market_probability: float = 0.5
    edge: float = 0.0
    direction: str = "yes"               # bet side: yes|no
    confidence: float = 0.5
    kelly_fraction: float = 0.0
    suggested_size: float = 0.0
    reasoning: str = ""

    ensemble_mean: float = 0.0
    ensemble_std: float = 0.0
    ensemble_members: int = 0
    low_confidence_flag: bool = False

    source_probs: Dict[str, float] = field(default_factory=dict)
    agreement: str = "MEDIUM"
    sources_used: List[str] = field(default_factory=list)
    outlier_dampened: Optional[str] = None
    filter_reason: str = ""

    min_edge_threshold: float = 0.08
    low_conf_edge_override: float = 0.15

    @property
    def passes_paper_threshold(self) -> bool:
        return abs(self.edge) >= self.min_edge_threshold

    @property
    def passes_threshold(self) -> bool:
        edge_threshold = self.min_edge_threshold
        if self.low_confidence_flag or self.agreement == "LOW":
            edge_threshold = max(edge_threshold, self.low_conf_edge_override)
        return abs(self.edge) >= edge_threshold


def decide(day: WeatherDay, params: StrategyParams, bankroll: Optional[float] = None) -> Optional[Decision]:
    """Pure decision core. Returns a Decision, or None when there's no usable data."""
    market = day.market
    as_of = day.as_of
    if bankroll is None:
        bankroll = params.initial_bankroll

    if market.metric == "rain":
        return _decide_rain(day, params, bankroll)

    raw_sources = day.sources or {}
    multi_result: Optional[MultiSourceResult] = None

    # ── GFS vs ECMWF divergence filter ───────────────────────────────────────
    _gfs_src = raw_sources.get("gfs")
    _ecmwf_src = raw_sources.get("ecmwf")
    if (_gfs_src and _gfs_src.ok and _gfs_src.member_highs
            and _ecmwf_src and _ecmwf_src.ok and _ecmwf_src.member_highs):
        _gfs_members = _gfs_src.member_highs if market.metric == "high" else _gfs_src.member_lows
        _ecmwf_members = _ecmwf_src.member_highs if market.metric == "high" else _ecmwf_src.member_lows
        if _gfs_members and _ecmwf_members:
            _gfs_mean = statistics.mean(_gfs_members)
            _ecmwf_mean = statistics.mean(_ecmwf_members)
            _divergence = abs(_gfs_mean - _ecmwf_mean)
            if _divergence > params.model_divergence_threshold:
                return Decision(
                    market=market, model_probability=0.5,
                    market_probability=market.yes_price, edge=0.0, direction="yes",
                    confidence=0.3, suggested_size=0.0,
                    reasoning=f"[FILTERED:model_divergence] GFS={_gfs_mean:.1f}F ECMWF={_ecmwf_mean:.1f}F diff={_divergence:.1f}F",
                    ensemble_mean=(_gfs_mean + _ecmwf_mean) / 2, ensemble_members=len(_gfs_members),
                    low_confidence_flag=True, agreement="LOW", sources_used=["gfs", "ecmwf"],
                    filter_reason="model_divergence",
                    min_edge_threshold=params.min_edge_threshold,
                    low_conf_edge_override=params.low_confidence_edge_override,
                )

    # ── Metric remap + grid bias ──────────────────────────────────────────────
    grid_bias = day.grid_bias
    metric_sources: Dict[str, SourceForecast] = {}
    for name, src in raw_sources.items():
        if not src.ok:
            metric_sources[name] = src
            continue
        members = src.member_highs if market.metric == "high" else src.member_lows
        if grid_bias != 0.0:
            members = [m + grid_bias for m in members]
        metric_sources[name] = SourceForecast(
            source=name, member_highs=members, member_lows=members,
            ok=bool(members), error="" if members else f"no {market.metric} data",
        )

    if metric_sources:
        multi_result = compute_multi_source_probability(
            sources=metric_sources, threshold_f=market.threshold_f,
            direction=market.direction, target_date=market.target_date,
            metric=market.metric, city_key=market.city_key,
            params=params, as_of=as_of,
        )

    # ── Single-source GFS fallback ────────────────────────────────────────────
    if multi_result is None:
        gfs = raw_sources.get("gfs")
        member_values = None
        if gfs and gfs.ok:
            member_values = gfs.member_highs if market.metric == "high" else gfs.member_lows
        if not member_values:
            return None
        prob_result = compute_probability(
            member_values=member_values, threshold_f=market.threshold_f,
            direction=market.direction, target_date=market.target_date,
            metric=market.metric, params=params, as_of=as_of,
        )
        if not prob_result:
            return None
        model_yes_prob = prob_result.model_prob
        ensemble_mean = prob_result.ensemble_mean
        ensemble_std = prob_result.ensemble_std
        ensemble_members = len(member_values)
        low_conf = prob_result.low_confidence_flag
        confidence = prob_result.confidence
        source_probs_map = {"gfs": model_yes_prob}
        agreement = "MEDIUM"
        sources_used = ["gfs"]
    else:
        model_yes_prob = multi_result.combined_prob
        ensemble_mean = multi_result.ensemble_mean
        ensemble_std = multi_result.ensemble_std
        ref_src = multi_result.source_probs.get("gfs") or next(iter(multi_result.source_probs.values()))
        ensemble_members = ref_src.members
        low_conf = multi_result.low_confidence_flag
        confidence = multi_result.confidence
        source_probs_map = {k: v.prob for k, v in multi_result.source_probs.items()}
        agreement = multi_result.agreement
        sources_used = list(multi_result.source_probs.keys())

    def _filtered(reason: str, **extra) -> Decision:
        base = dict(
            market=market, model_probability=model_yes_prob,
            market_probability=market.yes_price, edge=0.0, direction="yes",
            confidence=confidence, suggested_size=0.0,
            ensemble_mean=ensemble_mean, ensemble_std=ensemble_std,
            ensemble_members=ensemble_members, low_confidence_flag=low_conf,
            source_probs=source_probs_map, agreement=agreement, sources_used=sources_used,
            outlier_dampened=multi_result.outlier_dampened if multi_result else None,
            filter_reason=reason,
            min_edge_threshold=params.min_edge_threshold,
            low_conf_edge_override=params.low_confidence_edge_override,
        )
        base.update(extra)
        return Decision(**base)

    # ── Observation constraint (within obs window) ────────────────────────────
    if market.metric in ("high", "low") and day.observation:
        _resolution_et = datetime(market.target_date.year, market.target_date.month,
                                  market.target_date.day, 23, 59, 0, tzinfo=ET)
        _hours_left = (_resolution_et.astimezone(timezone.utc) - as_of.astimezone(timezone.utc)).total_seconds() / 3600.0
        if 0 < _hours_left <= params.obs_window_hours:
            obs = day.observation
            obs_max = obs.get("observed_max_f")
            obs_min = obs.get("observed_min_f")
            if market.metric == "high" and obs_max is not None:
                if market.direction == "below" and obs_max >= market.threshold_f:
                    return _filtered("obs_resolved")
                if market.direction == "above" and obs_max >= market.threshold_f:
                    return _filtered("obs_guaranteed")
            if market.metric == "low" and obs_min is not None:
                if market.direction == "above" and obs_min < market.threshold_f:
                    return _filtered("obs_resolved")
                if market.direction == "below" and obs_min < market.threshold_f:
                    return _filtered("obs_guaranteed")

    # ── Climatology prior filter ──────────────────────────────────────────────
    climo_normal = day.climatology_normal
    if climo_normal is not None and market.metric in ("high", "low"):
        if abs(ensemble_mean - climo_normal) <= params.climatology_deviation_max:
            return _filtered("near_climatology")

    # ── Market implied probability ────────────────────────────────────────────
    if market.yes_bid > 0:
        market_yes_prob = (market.yes_ask + market.yes_bid) / 2.0
    else:
        market_yes_prob = market.yes_price

    # ── Trading-hours gates (with high-conviction bypass) ─────────────────────
    now_et = as_of.astimezone(ET)
    thresh = params.conviction_threshold
    high_conviction = (
        (model_yes_prob >= thresh + 0.10 and market_yes_prob >= thresh)
        or (model_yes_prob <= 1.0 - (thresh + 0.10) and market_yes_prob <= 1.0 - thresh)
    )

    stale_reason = _staleness(now_et, params)
    if stale_reason and not high_conviction:
        return None

    if market.metric in ("high", "low") and market.target_date == now_et.date():
        cutoff = params.same_day_high_cutoff_hour if market.metric == "high" else params.same_day_low_cutoff_hour
        if now_et.hour >= cutoff and not high_conviction:
            return None

    # ── Edge, entry filters, Kelly ────────────────────────────────────────────
    # Direction from the model vs the market mid (mid used ONLY for direction).
    edge_vs_mid = model_yes_prob - market_yes_prob
    direction = "yes" if edge_vs_mid >= 0 else "no"
    entry_price = market.yes_price if direction == "yes" else market.no_price
    # Execution-aware edge: score against the price we'd actually PAY (entry),
    # not the mid — mirrors weather_signals.py so live and backtest stay in sync.
    edge = (model_yes_prob - entry_price) if direction == "yes" \
        else ((1.0 - model_yes_prob) - entry_price)

    nws_prob = source_probs_map.get("nws", 0.0)
    cold_margin = (market.threshold_f - ensemble_mean) if market.direction == "below" else 0.0
    cold_day_exception = (
        direction == "yes" and market.direction == "below"
        and cold_margin >= params.cold_day_margin and nws_prob >= params.cold_day_nws_min
    )
    yes_min_entry = params.entry_min_price if cold_day_exception else params.yes_entry_floor
    entry_too_high = entry_price > params.entry_max_price
    entry_too_low = (entry_price < yes_min_entry if direction == "yes"
                     else entry_price < params.entry_min_price)

    if model_yes_prob == params.prob_floor:
        sizing_prob = params.prob_floor_sizing
    elif model_yes_prob == params.prob_ceiling:
        sizing_prob = params.prob_ceiling_sizing
    else:
        sizing_prob = model_yes_prob
    suggested_size = kelly_size(
        model_prob=sizing_prob, entry_price=entry_price, direction=direction,
        bankroll=bankroll, kelly_fraction=params.kelly_fraction, fee_rate=params.taker_fee_coef,
    )
    suggested_size = min(suggested_size, params.max_trade_size)

    entry_price_filtered = entry_too_high or entry_too_low
    if entry_price_filtered:
        edge = 0.0

    min_edge = min_profitable_edge(entry_price, fee_coef=params.taker_fee_coef)
    req_edge = params.low_confidence_edge_override if (low_conf or agreement == "LOW") else params.min_edge_threshold
    status = "ACTIONABLE" if abs(edge) >= req_edge else "FILTERED"
    reasoning = (
        f"[{status}] {market.city_name} {market.metric} {market.direction} "
        f"{market.threshold_f:.0f}F on {market.target_date} | model={model_yes_prob:.0%} "
        f"market={market_yes_prob:.0%} edge={edge:+.1%} → {direction.upper()} @ {entry_price:.0%} "
        f"| min_edge={min_edge:.1%} agree={agreement}"
    )

    return Decision(
        market=market, model_probability=model_yes_prob, market_probability=market_yes_prob,
        edge=edge, direction=direction, confidence=confidence,
        kelly_fraction=suggested_size / bankroll if bankroll > 0 else 0,
        suggested_size=suggested_size, reasoning=reasoning,
        ensemble_mean=ensemble_mean, ensemble_std=ensemble_std, ensemble_members=ensemble_members,
        low_confidence_flag=low_conf, source_probs=source_probs_map, agreement=agreement,
        sources_used=sources_used,
        outlier_dampened=multi_result.outlier_dampened if multi_result else None,
        filter_reason=("cold_day_exception" if cold_day_exception and not entry_price_filtered
                       else "entry_price" if entry_price_filtered else ""),
        min_edge_threshold=params.min_edge_threshold,
        low_conf_edge_override=params.low_confidence_edge_override,
    )


def _staleness(now_et: datetime, params: StrategyParams) -> Optional[str]:
    """Reason string if the last model run is older than the staleness limit, else None."""
    min_hours = float("inf")
    for h, m in params.model_run_hours_et:
        run_dt = now_et.replace(hour=h, minute=m, second=0, microsecond=0)
        from datetime import timedelta
        if run_dt > now_et:
            run_dt -= timedelta(days=1)
        min_hours = min(min_hours, (now_et - run_dt).total_seconds() / 3600.0)
    if min_hours > params.model_data_max_age_hours:
        return f"stale_model_data ({min_hours:.1f}h)"
    return None


def _decide_rain(day: WeatherDay, params: StrategyParams, bankroll: float) -> Optional[Decision]:
    """Pure decision core for binary rain markets — uses day.rain_prob."""
    market = day.market
    rain_prob = day.rain_prob
    if rain_prob is None:
        return None
    model_yes_prob = max(params.prob_floor, min(params.prob_ceiling, rain_prob))
    market_yes_prob = market.yes_price
    direction = "yes" if (model_yes_prob - market_yes_prob) >= 0 else "no"
    entry_price = market.yes_price if direction == "yes" else market.no_price
    # Execution-aware edge: score against the price actually paid (entry).
    edge = (model_yes_prob - entry_price) if direction == "yes" \
        else ((1.0 - model_yes_prob) - entry_price)
    if entry_price > params.entry_max_price or entry_price < params.rain_entry_floor:
        edge = 0.0
    if model_yes_prob == params.prob_floor:
        sizing_prob = params.prob_floor_sizing
    elif model_yes_prob == params.prob_ceiling:
        sizing_prob = params.prob_ceiling_sizing
    else:
        sizing_prob = model_yes_prob
    suggested_size = kelly_size(
        model_prob=sizing_prob, entry_price=entry_price, direction=direction,
        bankroll=bankroll, kelly_fraction=params.kelly_fraction, fee_rate=params.taker_fee_coef,
    )
    suggested_size = min(suggested_size, params.max_trade_size)
    return Decision(
        market=market, model_probability=model_yes_prob, market_probability=market_yes_prob,
        edge=edge, direction=direction, confidence=0.6,
        kelly_fraction=suggested_size / bankroll if bankroll > 0 else 0,
        suggested_size=suggested_size,
        reasoning=f"[RAIN] {market.city_name} {market.target_date} precip={rain_prob:.0%} edge={edge:+.1%}",
        ensemble_mean=rain_prob * 100, ensemble_members=1,
        source_probs={"gfs": model_yes_prob}, agreement="MEDIUM", sources_used=["gfs"],
        min_edge_threshold=params.min_edge_threshold,
        low_conf_edge_override=params.low_confidence_edge_override,
    )
