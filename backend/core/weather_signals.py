"""Signal generator using ensemble-of-ensembles (GFS + ECMWF + GEM + NWS)."""
import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional

from backend.config import settings
from backend.core.probability import (
    compute_multi_source_probability,
    compute_probability,
    kelly_size,
    min_profitable_edge,
    MultiSourceResult,
    LOW_CONFIDENCE_EDGE_OVERRIDE,
)
from backend.data.weather import fetch_ensemble_forecast, CITY_CONFIG
from backend.data.weather_markets import WeatherMarket
from backend.models.database import SessionLocal, Signal

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

    @property
    def passes_threshold(self) -> bool:
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
        parts = []
        for name in ["gfs", "ecmwf", "gem", "nws"]:
            if name in sp:
                parts.append(f"{name.upper()}={sp[name].prob:.0%}")
        reasoning_sources = (
            f"Combined({multi_result.combined_prob:.0%}) "
            f"[{' | '.join(parts)}] "
            f"spread={multi_result.max_spread:.0%} agreement={agreement}"
        )

    market_yes_prob = market.yes_price

    # Edge direction
    edge = model_yes_prob - market_yes_prob
    direction = "yes" if edge >= 0 else "no"

    # Entry price filter
    entry_price = market.yes_price if direction == "yes" else market.no_price
    entry_price_filtered = entry_price > settings.WEATHER_MAX_ENTRY_PRICE

    # Kelly sizing
    suggested_size = kelly_size(
        model_prob=model_yes_prob,
        market_price=market_yes_prob,
        direction=direction,
        bankroll=settings.INITIAL_BANKROLL,
        kelly_fraction=settings.KELLY_FRACTION,
        fee_rate=settings.KALSHI_FEE_RATE,
    )
    suggested_size = min(suggested_size, settings.WEATHER_MAX_TRADE_SIZE)

    if entry_price_filtered:
        edge = 0.0

    # Reasoning string
    min_edge = min_profitable_edge(settings.KALSHI_FEE_RATE)
    req_edge = LOW_CONFIDENCE_EDGE_OVERRIDE if (low_conf or agreement == "LOW") else settings.MIN_EDGE_THRESHOLD
    status = "ACTIONABLE" if abs(edge) >= req_edge else "FILTERED"

    filter_notes = []
    if entry_price_filtered:
        filter_notes.append(f"entry {entry_price:.0%} > max {settings.WEATHER_MAX_ENTRY_PRICE:.0%}")
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

    if entry_price > settings.WEATHER_MAX_ENTRY_PRICE:
        edge = 0.0

    suggested_size = kelly_size(
        model_prob=model_yes_prob,
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


async def scan_for_weather_signals() -> List[WeatherTradingSignal]:
    """Scan Kalshi weather markets and generate ensemble-of-ensembles signals."""
    from backend.data.kalshi_client import kalshi_credentials_present
    from backend.data.kalshi_markets import fetch_kalshi_weather_markets

    city_keys = [c.strip() for c in settings.WEATHER_CITIES.split(",") if c.strip()]

    logger.info("=" * 60)
    logger.info("WEATHER SCAN: Fetching Kalshi temperature markets...")

    markets: List[WeatherMarket] = []
    if not kalshi_credentials_present():
        logger.warning("Kalshi credentials not configured — skipping market fetch")
    else:
        try:
            kalshi_markets = await fetch_kalshi_weather_markets(city_keys)
            markets.extend(kalshi_markets)
            logger.info(f"Kalshi: found {len(kalshi_markets)} weather markets")
        except Exception as e:
            logger.error(f"Failed to fetch Kalshi weather markets: {e}", exc_info=True)

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

    actionable = [s for s in signals if s.passes_threshold]
    logger.info(
        f"SCAN COMPLETE: {len(markets)} markets → {len(signals)} signals, "
        f"{len(actionable)} actionable (edge >= {settings.MIN_EDGE_THRESHOLD:.0%})"
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
    return signals


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
