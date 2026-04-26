"""Signal generator using ensemble-of-ensembles (GFS + ECMWF + GEM + NWS)."""
import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional

from backend.config import settings
from backend.core.probability import (
    compute_multi_source_probability,
    compute_probability,
    kelly_size,
    min_profitable_edge,
    MultiSourceResult,
    LOW_CONFIDENCE_EDGE_OVERRIDE,
)

# When model_prob hits the 0.05/0.95 probability floor it means the raw blend
# wanted to go even more extreme, but we clamp it.  Those trades win ~45% of the
# time in practice despite the model claiming 95% confidence, so we use a less
# extreme stand-in probability *only for Kelly sizing* to avoid over-sizing.
from backend.data.weather import fetch_ensemble_forecast, CITY_CONFIG, get_climatology_normal
from backend.data.weather_markets import WeatherMarket
from backend.models.database import SessionLocal, Signal

# When model_prob hits the 0.05/0.95 probability floor it means the raw blend
# wanted to go even more extreme, but we clamp it.  Those trades win ~45% of the
# time in practice despite the model claiming 95% confidence, so we use a less
# extreme stand-in probability *only for Kelly sizing* to avoid over-sizing.
PROB_FLOOR          = 0.05
PROB_CEILING        = 0.95
PROB_FLOOR_SIZING   = 0.15   # substitute when model_prob == PROB_FLOOR
PROB_CEILING_SIZING = 0.85   # substitute when model_prob == PROB_CEILING

logger = logging.getLogger("weatherbot")


@dataclass
class WeatherTradingSignal:
    """A trading signal for a Kalshi weather temperature market."""
    market: WeatherMarket

    model_probability: float = 0.5
    market_probability: float = 0.5
    edge: float = 0.0
    direction: str = "yes"

    confidence: float = 0.5
    kelly_fraction: float = 0.0
    suggested_size: float = 0.0

    reasoning: str = ""
    timestamp: datetime = field(default_factory=datetime.utcnow)

    ensemble_mean: float = 0.0
    ensemble_std: float = 0.0
    ensemble_members: int = 0
    low_confidence_flag: bool = False

    # Multi-source breakdown (None when only single-source GFS available)
    source_probs: Dict[str, float] = field(default_factory=dict)   # {source: P(YES)}
    agreement: str = "MEDIUM"   # "HIGH", "MEDIUM", "LOW"
    sources_used: List[str] = field(default_factory=list)
    outlier_dampened: Optional[str] = None   # model name if outlier dampening was applied

    # Set after scan — populated for signals that didn't pass threshold
    filter_reason: str = ""   # "low_agreement", "below_edge", "entry_price"

    @property
    def passes_paper_threshold(self) -> bool:
        """Always just MIN_EDGE_THRESHOLD — used for paper trading to capture all signals."""
        return abs(self.edge) >= settings.MIN_EDGE_THRESHOLD

    @property
    def passes_threshold(self) -> bool:
        """Live trading threshold — raises bar to 15% for LOW agreement signals."""
        edge_threshold = settings.MIN_EDGE_THRESHOLD
        if self.low_confidence_flag or self.agreement == "LOW":
            edge_threshold = max(edge_threshold, LOW_CONFIDENCE_EDGE_OVERRIDE)
        return abs(self.edge) >= edge_threshold


async def generate_weather_signal(market: WeatherMarket) -> Optional[WeatherTradingSignal]:
    """
    Generate a trading signal using ensemble-of-ensembles probability model.

    Fetches GFS, ECMWF, GEM, and NWS forecasts in parallel. Falls back
    gracefully to single-source GFS if multi-source fetch fails.
    """
    # ── Rain markets: use precipitation probability directly ──────────────────
    if market.metric == "rain":
        return await _generate_rain_signal(market)

    # ── Try multi-source first ────────────────────────────────────────────────
    multi_result: Optional[MultiSourceResult] = None

    try:
        from backend.data.multi_source_weather import fetch_all_sources

        raw_sources = await fetch_all_sources(market.city_key, market.target_date)

        # ── GFS vs ECMWF divergence filter ───────────────────────────────
        # If the two primary models disagree by more than 8°F on the ensemble
        # mean, there is a major pattern conflict — skip to avoid whipsaw.
        _MODEL_DIVERGENCE_THRESHOLD = 8.0
        _gfs_src  = raw_sources.get("gfs")
        _ecmwf_src = raw_sources.get("ecmwf")
        if (
            _gfs_src and _gfs_src.ok and _gfs_src.member_highs
            and _ecmwf_src and _ecmwf_src.ok and _ecmwf_src.member_highs
        ):
            import statistics as _st
            _gfs_members  = _gfs_src.member_highs  if market.metric == "high" else _gfs_src.member_lows
            _ecmwf_members = _ecmwf_src.member_highs if market.metric == "high" else _ecmwf_src.member_lows
            if _gfs_members and _ecmwf_members:
                _gfs_mean   = _st.mean(_gfs_members)
                _ecmwf_mean = _st.mean(_ecmwf_members)
                _divergence = abs(_gfs_mean - _ecmwf_mean)
                if _divergence > _MODEL_DIVERGENCE_THRESHOLD:
                    logger.info(
                        f"SKIP {market.market_id}: GFS mean={_gfs_mean:.1f}F vs "
                        f"ECMWF mean={_ecmwf_mean:.1f}F — divergence {_divergence:.1f}F "
                        f"exceeds {_MODEL_DIVERGENCE_THRESHOLD:.0f}F threshold (model_divergence)"
                    )
                    # Return a filtered signal (edge=0) rather than None so it
                    # shows up in the scan report for visibility.
                    return WeatherTradingSignal(
                        market=market,
                        model_probability=0.5,
                        market_probability=market.yes_price,
                        edge=0.0,
                        direction="yes",
                        confidence=0.3,
                        kelly_fraction=0.0,
                        suggested_size=0.0,
                        reasoning=(
                            f"[FILTERED:model_divergence] GFS={_gfs_mean:.1f}F "
                            f"ECMWF={_ecmwf_mean:.1f}F diff={_divergence:.1f}F"
                        ),
                        ensemble_mean=(_gfs_mean + _ecmwf_mean) / 2,
                        ensemble_std=0.0,
                        ensemble_members=len(_gfs_members),
                        low_confidence_flag=True,
                        source_probs={},
                        agreement="LOW",
                        sources_used=["gfs", "ecmwf"],
                        filter_reason="model_divergence",
                    )

        # Re-map member list to correct metric (highs vs lows)
        from backend.data.multi_source_weather import SourceForecast
        metric_sources: Dict[str, SourceForecast] = {}
        for name, src in raw_sources.items():
            if not src.ok:
                metric_sources[name] = src
                continue
            members = src.member_highs if market.metric == "high" else src.member_lows
            metric_sources[name] = SourceForecast(
                source=name,
                member_highs=members,
                member_lows=members,
                ok=bool(members),
                error="" if members else f"no {market.metric} data",
            )

        multi_result = compute_multi_source_probability(
            sources=metric_sources,
            threshold_f=market.threshold_f,
            direction=market.direction,
            target_date=market.target_date,
            metric=market.metric,
            city_key=market.city_key,
        )
    except Exception as e:
        logger.warning(f"Multi-source fetch failed for {market.market_id}, falling back to GFS: {e}")

    # ── Fall back to single-source GFS ────────────────────────────────────────
    if multi_result is None:
        forecast = await fetch_ensemble_forecast(market.city_key, market.target_date)
        if not forecast:
            return None
        member_values = forecast.member_highs if market.metric == "high" else forecast.member_lows
        if not member_values:
            return None

        prob_result = compute_probability(
            member_values=member_values,
            threshold_f=market.threshold_f,
            direction=market.direction,
            target_date=market.target_date,
            metric=market.metric,
        )
        if not prob_result:
            return None

        model_yes_prob    = prob_result.model_prob
        ensemble_mean     = prob_result.ensemble_mean
        ensemble_std      = prob_result.ensemble_std
        ensemble_members  = len(member_values)
        low_conf          = prob_result.low_confidence_flag
        confidence        = prob_result.confidence
        source_probs_map  = {"gfs": model_yes_prob}
        agreement         = "MEDIUM"
        sources_used      = ["gfs"]
        reasoning_sources = f"GFS only (multi-source unavailable)"
    else:
        model_yes_prob    = multi_result.combined_prob
        ensemble_mean     = multi_result.ensemble_mean
        ensemble_std      = multi_result.ensemble_std
        ref_src           = multi_result.source_probs.get("gfs") or next(iter(multi_result.source_probs.values()))
        ensemble_members  = ref_src.members
        low_conf          = multi_result.low_confidence_flag
        confidence        = multi_result.confidence
        source_probs_map  = {k: v.prob for k, v in multi_result.source_probs.items()}
        agreement         = multi_result.agreement
        sources_used      = list(multi_result.source_probs.keys())

        sp = multi_result.source_probs
        wt = multi_result.weights_used
        parts = []
        for name in ["gfs", "ecmwf", "gem", "nws"]:
            if name in sp:
                dampened = "⚡" if name == multi_result.outlier_dampened else ""
                parts.append(f"{name.upper()}={sp[name].prob:.0%}(w={wt.get(name,0):.2f}{dampened})")
        reasoning_sources = (
            f"Combined({multi_result.combined_prob:.0%}) "
            f"[{' | '.join(parts)}] "
            f"spread={multi_result.max_spread:.0%} agreement={agreement}"
            + (f" outlier_dampened={multi_result.outlier_dampened}" if multi_result.outlier_dampened else "")
        )

    # ── Real-time observation constraint (≤6 hours to expiry) ────────────────
    # When the market resolves very soon, actual observations may already
    # determine the outcome. Fetch current NWS observation and short-circuit
    # if the result is already decided.
    if market.metric in ("high", "low"):
        try:
            from backend.data.multi_source_weather import fetch_current_observation
            from datetime import timezone as _tz
            from zoneinfo import ZoneInfo as _ZI

            # Hours until market resolves (end of target_date in ET)
            _et = _ZI("America/New_York")
            _resolution_et = datetime(
                market.target_date.year, market.target_date.month, market.target_date.day,
                23, 59, 0, tzinfo=_et,
            )
            _now_utc = datetime.now(_tz.utc)
            _hours_left = (_resolution_et.astimezone(_tz.utc) - _now_utc).total_seconds() / 3600.0

            if 0 < _hours_left <= 6:
                obs = await fetch_current_observation(market.city_key)
                if obs:
                    obs_max = obs["observed_max_f"]
                    obs_min = obs["observed_min_f"]
                    obs_cur = obs["current_temp_f"]
                    logger.info(
                        f"OBS {market.city_key} ({market.target_date}): "
                        f"current={obs_cur:.1f}F max={obs_max:.1f}F min={obs_min:.1f}F "
                        f"hours_left={_hours_left:.1f}h"
                    )

                    if market.metric == "high":
                        if market.direction == "below" and obs_max >= market.threshold_f:
                            # Temp already exceeded threshold — YES (below X) has already lost
                            logger.info(
                                f"SKIP {market.market_id}: observed_max={obs_max:.1f}F >= "
                                f"threshold={market.threshold_f:.0f}F — market already resolved NO"
                            )
                            _sig = WeatherTradingSignal(
                                market=market, model_probability=model_yes_prob,
                                market_probability=market.yes_price, edge=0.0,
                                direction="yes", confidence=confidence,
                                kelly_fraction=0.0, suggested_size=0.0,
                                reasoning=f"[FILTERED:obs_resolved] observed_max={obs_max:.1f}F >= threshold={market.threshold_f:.0f}F",
                                ensemble_mean=ensemble_mean, ensemble_std=ensemble_std,
                                ensemble_members=ensemble_members,
                                low_confidence_flag=low_conf,
                                source_probs=source_probs_map, agreement=agreement,
                                sources_used=sources_used,
                                outlier_dampened=multi_result.outlier_dampened if multi_result else None,
                                filter_reason="obs_resolved",
                            )
                            return _sig

                        if market.direction == "above" and obs_max >= market.threshold_f:
                            # Already above threshold — YES (above X) has already won; market ≈ 99¢
                            logger.info(
                                f"SKIP {market.market_id}: observed_max={obs_max:.1f}F >= "
                                f"threshold={market.threshold_f:.0f}F — YES already guaranteed, skip"
                            )
                            _sig = WeatherTradingSignal(
                                market=market, model_probability=model_yes_prob,
                                market_probability=market.yes_price, edge=0.0,
                                direction="yes", confidence=confidence,
                                kelly_fraction=0.0, suggested_size=0.0,
                                reasoning=f"[FILTERED:obs_guaranteed] observed_max={obs_max:.1f}F >= threshold={market.threshold_f:.0f}F",
                                ensemble_mean=ensemble_mean, ensemble_std=ensemble_std,
                                ensemble_members=ensemble_members,
                                low_confidence_flag=low_conf,
                                source_probs=source_probs_map, agreement=agreement,
                                sources_used=sources_used,
                                outlier_dampened=multi_result.outlier_dampened if multi_result else None,
                                filter_reason="obs_guaranteed",
                            )
                            return _sig

        except Exception as _obs_err:
            logger.debug(f"Observation constraint check failed for {market.market_id}: {_obs_err}")

    # ── Climatology prior filter ──────────────────────────────────────────────
    # If the ensemble mean is within 1.5°F of the 30-year monthly normal,
    # the model has no real edge — the market already prices in climatology.
    climo_normal = get_climatology_normal(market.city_key, market.target_date, market.metric)
    if climo_normal is not None and market.metric in ("high", "low"):
        deviation = abs(ensemble_mean - climo_normal)
        if deviation <= 1.5:
            logger.info(
                f"SKIP {market.market_id}: ensemble mean={ensemble_mean:.1f}F is within "
                f"1.5F of climatology normal={climo_normal:.0f}F (deviation={deviation:.1f}F) "
                f"— near_climatology filter"
            )
            signal = WeatherTradingSignal(
                market=market,
                model_probability=model_yes_prob,
                market_probability=market.yes_price,
                edge=0.0,
                direction="yes",
                confidence=confidence,
                kelly_fraction=0.0,
                suggested_size=0.0,
                reasoning=f"[FILTERED:near_climatology] ensemble={ensemble_mean:.1f}F normal={climo_normal:.0f}F deviation={deviation:.1f}F",
                ensemble_mean=ensemble_mean,
                ensemble_std=ensemble_std,
                ensemble_members=ensemble_members,
                low_confidence_flag=low_conf,
                source_probs=source_probs_map,
                agreement=agreement,
                sources_used=sources_used,
                outlier_dampened=multi_result.outlier_dampened if multi_result else None,
                filter_reason="near_climatology",
            )
            return signal

    # Use bid/ask midpoint as the market's implied probability when bid is
    # available (more accurate). Fall back to yes_price (the ask) when bid
    # is unavailable — which is the common case for Kalshi's bulk endpoint.
    # Entry cost for Kelly sizing always uses the ask (yes_price / no_price).
    if market.yes_bid > 0:
        market_yes_prob = (market.yes_ask + market.yes_bid) / 2.0
    else:
        market_yes_prob = market.yes_price  # ask only — bid not exposed by API

    # Edge direction
    edge = model_yes_prob - market_yes_prob
    direction = "yes" if edge >= 0 else "no"

    # High temp YES bets are systematically miscalibrated (14% empirical win rate
    # vs model-implied 60-95%). Block them entirely.
    if market.metric == "high" and direction == "yes":
        return WeatherTradingSignal(
            market=market,
            model_probability=model_yes_prob,
            market_probability=market_yes_prob,
            edge=0.0,
            direction="yes",
            confidence=confidence,
            kelly_fraction=0.0,
            suggested_size=0.0,
            reasoning=(
                f"[FILTERED:high_yes_blocked] {market.city_name} high YES bets "
                f"blocked — model overestimates warm days (empirical win rate 14%)"
            ),
            ensemble_mean=ensemble_mean,
            ensemble_std=ensemble_std,
            ensemble_members=ensemble_members,
            low_confidence_flag=low_conf,
            source_probs=source_probs_map,
            agreement=agreement,
            sources_used=sources_used,
            outlier_dampened=multi_result.outlier_dampened if multi_result else None,
            filter_reason="high_yes_blocked",
        )

    # Entry price filters — too cheap or too expensive
    entry_price = market.yes_price if direction == "yes" else market.no_price
    entry_too_high = entry_price > settings.WEATHER_MAX_ENTRY_PRICE
    entry_too_low  = entry_price < settings.WEATHER_MIN_ENTRY_PRICE

    # Kelly sizing — use a capped probability when the model hit the floor/ceiling
    # so that extreme-confidence signals don't get outsized positions.
    if model_yes_prob == PROB_FLOOR:
        sizing_prob = PROB_FLOOR_SIZING
    elif model_yes_prob == PROB_CEILING:
        sizing_prob = PROB_CEILING_SIZING
    else:
        sizing_prob = model_yes_prob
    suggested_size = kelly_size(
        model_prob=sizing_prob,
        market_price=market_yes_prob,
        direction=direction,
        bankroll=settings.INITIAL_BANKROLL,
        kelly_fraction=settings.KELLY_FRACTION,
        fee_rate=settings.KALSHI_FEE_RATE,
    )
    suggested_size = min(suggested_size, settings.WEATHER_MAX_TRADE_SIZE)

    entry_price_filtered = entry_too_high or entry_too_low
    if entry_price_filtered:
        edge = 0.0

    # Reasoning string
    min_edge = min_profitable_edge(settings.KALSHI_FEE_RATE)
    req_edge = LOW_CONFIDENCE_EDGE_OVERRIDE if (low_conf or agreement == "LOW") else settings.MIN_EDGE_THRESHOLD
    status = "ACTIONABLE" if abs(edge) >= req_edge else "FILTERED"

    filter_notes = []
    if entry_too_high:
        filter_notes.append(f"entry {entry_price:.0%} > max {settings.WEATHER_MAX_ENTRY_PRICE:.0%}")
    if entry_too_low:
        filter_notes.append(f"entry {entry_price:.0%} < min {settings.WEATHER_MIN_ENTRY_PRICE:.0%}")
    if agreement == "LOW":
        filter_notes.append(f"models disagree ({req_edge:.0%} edge required)")
    filter_note = f" [{', '.join(filter_notes)}]" if filter_notes else ""

    reasoning = (
        f"[{status}]{filter_note} "
        f"{market.city_name} {market.metric} {market.direction} {market.threshold_f:.0f}F "
        f"on {market.target_date} | {reasoning_sources} | "
        f"mean={ensemble_mean:.1f}F std={ensemble_std:.1f}F | "
        f"Market: {market_yes_prob:.0%} | Edge: {edge:+.1%} → {direction.upper()} "
        f"@ {entry_price:.0%} | Min edge: {min_edge:.1%} | Conf: {confidence:.0%}"
    )

    return WeatherTradingSignal(
        market=market,
        model_probability=model_yes_prob,
        market_probability=market_yes_prob,
        edge=edge,
        direction=direction,
        confidence=confidence,
        kelly_fraction=suggested_size / settings.INITIAL_BANKROLL if settings.INITIAL_BANKROLL > 0 else 0,
        suggested_size=suggested_size,
        reasoning=reasoning,
        ensemble_mean=ensemble_mean,
        ensemble_std=ensemble_std,
        ensemble_members=ensemble_members,
        low_confidence_flag=low_conf,
        source_probs=source_probs_map,
        agreement=agreement,
        sources_used=sources_used,
        outlier_dampened=multi_result.outlier_dampened if multi_result else None,
        filter_reason="entry_price" if entry_price_filtered else "",
    )


async def _generate_rain_signal(market: WeatherMarket) -> Optional[WeatherTradingSignal]:
    """Generate signal for binary rain markets using GFS precipitation probability."""
    from backend.data.multi_source_weather import fetch_rain_probability

    rain_prob = await fetch_rain_probability(market.city_key, market.target_date)
    if rain_prob is None:
        return None

    # market.direction == "above" means YES = will rain
    model_yes_prob = max(0.05, min(0.95, rain_prob))
    market_yes_prob = market.yes_price

    edge = model_yes_prob - market_yes_prob
    direction = "yes" if edge >= 0 else "no"
    entry_price = market.yes_price if direction == "yes" else market.no_price

    # Rain NO bets at very low entry prices (YES priced >90¢) are strong opportunities
    # — use a tighter floor of 0.05 instead of the global WEATHER_MIN_ENTRY_PRICE.
    rain_min_entry = 0.05
    if entry_price > settings.WEATHER_MAX_ENTRY_PRICE or entry_price < rain_min_entry:
        edge = 0.0

    if model_yes_prob == PROB_FLOOR:
        rain_sizing_prob = PROB_FLOOR_SIZING
    elif model_yes_prob == PROB_CEILING:
        rain_sizing_prob = PROB_CEILING_SIZING
    else:
        rain_sizing_prob = model_yes_prob
    suggested_size = kelly_size(
        model_prob=rain_sizing_prob,
        market_price=market_yes_prob,
        direction=direction,
        bankroll=settings.INITIAL_BANKROLL,
        kelly_fraction=settings.KELLY_FRACTION,
        fee_rate=settings.KALSHI_FEE_RATE,
    )
    suggested_size = min(suggested_size, settings.WEATHER_MAX_TRADE_SIZE)

    reasoning = (
        f"[RAIN] {market.city_name} rain on {market.target_date} | "
        f"GFS precip prob={rain_prob:.0%} | Market={market_yes_prob:.0%} | "
        f"Edge={edge:+.1%} → {direction.upper()}"
    )

    return WeatherTradingSignal(
        market=market,
        model_probability=model_yes_prob,
        market_probability=market_yes_prob,
        edge=edge,
        direction=direction,
        confidence=0.6,
        kelly_fraction=suggested_size / settings.INITIAL_BANKROLL if settings.INITIAL_BANKROLL > 0 else 0,
        suggested_size=suggested_size,
        reasoning=reasoning,
        ensemble_mean=rain_prob * 100,
        ensemble_std=0.0,
        ensemble_members=1,
        source_probs={"gfs": model_yes_prob},
        agreement="MEDIUM",
        sources_used=["gfs"],
    )


@dataclass
class ScanReport:
    """Full scan output — markets, signals, and what was filtered at each stage."""
    signals: List[WeatherTradingSignal] = field(default_factory=list)
    # from MarketFetchReport
    fetch_report: Any = None   # MarketFetchReport
    # signal-level filtered (passed liquidity, but signal didn't make threshold)
    below_edge: List[WeatherTradingSignal] = field(default_factory=list)
    low_agreement_filtered: List[WeatherTradingSignal] = field(default_factory=list)

    @property
    def actionable(self) -> List[WeatherTradingSignal]:
        return [s for s in self.signals if s.passes_threshold]


async def scan_for_weather_signals() -> ScanReport:
    """Scan Kalshi weather markets and generate ensemble-of-ensembles signals."""
    from backend.data.kalshi_client import kalshi_credentials_present
    from backend.data.kalshi_markets import fetch_kalshi_weather_markets, MarketFetchReport

    city_keys = [c.strip() for c in settings.WEATHER_CITIES.split(",") if c.strip()] or None

    logger.info("=" * 60)
    logger.info("WEATHER SCAN: Fetching Kalshi temperature markets...")

    fetch_report = MarketFetchReport()
    if not kalshi_credentials_present():
        logger.warning("Kalshi credentials not configured — skipping market fetch")
    else:
        try:
            fetch_report = await fetch_kalshi_weather_markets(city_keys)
            logger.info(f"Kalshi: {len(fetch_report.markets)} markets passed filters")
        except Exception as e:
            logger.error(f"Failed to fetch Kalshi weather markets: {e}", exc_info=True)

    markets = fetch_report.markets
    logger.info(f"Total markets to analyze: {len(markets)}")

    signals: List[WeatherTradingSignal] = []
    for market in markets:
        try:
            signal = await generate_weather_signal(market)
            if signal:
                signals.append(signal)
        except Exception as e:
            logger.debug(f"Signal generation failed for {market.title}: {e}")

    signals.sort(key=lambda s: abs(s.edge), reverse=True)

    # ── Deduplicate correlated bets ───────────────────────────────────────────
    # For each city+date+metric group, only keep the single signal with the
    # highest absolute edge. Multiple thresholds for the same city/day resolve
    # on the same observed temperature — betting all of them is not
    # diversification, it multiplies the same correlated risk.
    signals = _dedup_correlated(signals)

    # ── Resize using live available bankroll ──────────────────────────────────
    # Kelly was computed with the static INITIAL_BANKROLL. Subtract capital
    # already committed to open (unresolved) paper trades so we never size
    # as if we have more money than we actually do.
    available = _available_bankroll()
    if available < settings.INITIAL_BANKROLL:
        ratio = available / settings.INITIAL_BANKROLL
        for s in signals:
            s.suggested_size = round(s.suggested_size * ratio, 2)
        logger.info(
            f"Bankroll scaling: {available:.2f}/{settings.INITIAL_BANKROLL:.2f} available "
            f"→ sizing multiplied by {ratio:.2f}"
        )

    # Annotate filter reason on each signal
    low_agreement_filtered = []
    below_edge = []
    for s in signals:
        if s.passes_threshold:
            continue
        req = LOW_CONFIDENCE_EDGE_OVERRIDE if (s.low_confidence_flag or s.agreement == "LOW") else settings.MIN_EDGE_THRESHOLD
        if s.agreement == "LOW" and abs(s.edge) >= settings.MIN_EDGE_THRESHOLD:
            s.filter_reason = "low_agreement"
            low_agreement_filtered.append(s)
        elif abs(s.edge) < req:
            s.filter_reason = "below_edge"
            below_edge.append(s)

    actionable = [s for s in signals if s.passes_threshold]
    logger.info(
        f"SCAN COMPLETE: {len(markets)} markets → {len(signals)} signals, "
        f"{len(actionable)} actionable, {len(low_agreement_filtered)} blocked by low agreement, "
        f"{len(below_edge)} below edge"
    )
    for s in actionable[:5]:
        src_str = "/".join(s.sources_used).upper()
        logger.info(
            f"  {s.market.city_name} {s.market.metric} {s.market.direction} "
            f"{s.market.threshold_f:.0f}F | Edge: {s.edge:+.1%} → {s.direction.upper()} "
            f"| {src_str} | Agreement: {s.agreement}"
            f"{'  ⚠' if s.low_confidence_flag else ''}"
        )

    _persist_signals(signals)

    report = ScanReport(
        signals=signals,
        fetch_report=fetch_report,
        below_edge=below_edge,
        low_agreement_filtered=low_agreement_filtered,
    )
    return report


def _dedup_correlated(signals: List[WeatherTradingSignal]) -> List[WeatherTradingSignal]:
    """
    For each (city, date, metric) group keep only the signal with the
    highest absolute edge. All thresholds for the same city+day+metric
    resolve on the same observed temperature, so they are perfectly
    correlated — holding more than one is just multiplying exposure, not
    diversifying.
    """
    best: dict = {}  # key -> signal with highest |edge|
    for s in signals:
        key = (s.market.city_key, s.market.target_date, s.market.metric)
        if key not in best or abs(s.edge) > abs(best[key].edge):
            best[key] = s

    kept = list(best.values())
    dropped = len(signals) - len(kept)
    if dropped:
        logger.info(
            f"Dedup: dropped {dropped} correlated signal(s) "
            f"({len(kept)} unique city+date+metric groups remain)"
        )
    return kept


def _available_bankroll() -> float:
    """
    Return bankroll minus capital currently locked in open paper trades.
    Falls back to INITIAL_BANKROLL if the DB is unreachable.
    """
    try:
        from backend.models.paper_trade import PaperSessionLocal, PaperTrade
        db = PaperSessionLocal()
        try:
            open_trades = db.query(PaperTrade).filter(PaperTrade.resolved == False).all()
            committed = sum(t.kelly_size for t in open_trades if t.kelly_size)
            available = max(0.0, settings.INITIAL_BANKROLL - committed)
            return available
        finally:
            db.close()
    except Exception as e:
        logger.debug(f"Could not compute available bankroll: {e}")
        return settings.INITIAL_BANKROLL


def _persist_signals(signals: List[WeatherTradingSignal]):
    """Save signals to DB — stores per-source probabilities in the JSON sources column."""
    to_save = [s for s in signals if abs(s.edge) > 0]
    if not to_save:
        return

    db = SessionLocal()
    try:
        for signal in to_save:
            existing = db.query(Signal).filter(
                Signal.market_ticker == signal.market.market_id,
                Signal.timestamp >= signal.timestamp.replace(second=0, microsecond=0),
            ).first()
            if existing:
                continue

            # Store per-source probs in the sources JSON column for calibration tracking
            sources_payload = {
                "models": signal.sources_used,
                "agreement": signal.agreement,
                "source_probs": signal.source_probs,
                "combined_prob": signal.model_probability,
            }

            db.add(Signal(
                market_ticker=signal.market.market_id,
                platform="kalshi",
                market_type="weather",
                timestamp=signal.timestamp,
                direction=signal.direction,
                model_probability=signal.model_probability,
                market_price=signal.market_probability,
                edge=signal.edge,
                confidence=signal.confidence,
                kelly_fraction=signal.kelly_fraction,
                suggested_size=signal.suggested_size,
                sources=sources_payload,
                reasoning=signal.reasoning,
                executed=False,
            ))

        db.commit()
    except Exception as e:
        logger.warning(f"Failed to persist signals: {e}")
        db.rollback()
    finally:
        db.close()
