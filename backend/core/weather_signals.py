"""Signal generator for Kalshi weather temperature markets using Gaussian CDF."""
import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional

from backend.config import settings
from backend.core.probability import compute_probability, kelly_size, min_profitable_edge
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
    direction: str = "yes"   # "yes" or "no"

    confidence: float = 0.5
    kelly_fraction: float = 0.0
    suggested_size: float = 0.0

    reasoning: str = ""
    timestamp: datetime = field(default_factory=datetime.utcnow)

    ensemble_mean: float = 0.0
    ensemble_std: float = 0.0
    ensemble_members: int = 0
    low_confidence_flag: bool = False

    @property
    def passes_threshold(self) -> bool:
        edge_threshold = settings.MIN_EDGE_THRESHOLD
        # Require higher edge if low_confidence_flag is set
        if self.low_confidence_flag:
            edge_threshold = max(edge_threshold, 0.12)
        return abs(self.edge) >= edge_threshold


async def generate_weather_signal(market: WeatherMarket) -> Optional[WeatherTradingSignal]:
    """Generate a trading signal using Gaussian CDF probability model."""
    forecast = await fetch_ensemble_forecast(market.city_key, market.target_date)
    if not forecast:
        return None

    # Select member values for the metric
    if market.metric == "high":
        member_values = forecast.member_highs
    else:
        member_values = forecast.member_lows

    if not member_values:
        return None

    # Compute Gaussian CDF probability
    prob_result = compute_probability(
        member_values=member_values,
        threshold_f=market.threshold_f,
        direction=market.direction,
        target_date=market.target_date,
    )
    if not prob_result:
        return None

    model_yes_prob = prob_result.model_prob
    market_yes_prob = market.yes_price

    # Edge = model_prob - market_price (for YES direction)
    # Positive edge → bet YES; negative edge → bet NO
    edge_yes = model_yes_prob - market_yes_prob
    if abs(edge_yes) >= abs(1.0 - model_yes_prob - (1.0 - market_yes_prob)):
        direction = "yes"
        edge = edge_yes
    else:
        direction = "no"
        edge = (1.0 - model_yes_prob) - (1.0 - market_yes_prob)

    # Simpler: edge is always model_yes_prob - market_yes_prob
    # Positive → YES is underpriced; Negative → NO is underpriced
    edge = model_yes_prob - market_yes_prob
    if edge >= 0:
        direction = "yes"
    else:
        direction = "no"

    # Entry price filter
    entry_price = market.yes_price if direction == "yes" else market.no_price
    entry_price_filtered = entry_price > settings.WEATHER_MAX_ENTRY_PRICE

    # Kelly sizing with fee adjustment
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
        edge = 0.0  # Zero out but still return for visibility

    # Build reasoning string
    min_edge = min_profitable_edge(settings.KALSHI_FEE_RATE)
    status = "ACTIONABLE" if abs(edge) >= settings.MIN_EDGE_THRESHOLD else "FILTERED"
    filter_notes = []
    if entry_price_filtered:
        filter_notes.append(f"entry {entry_price:.0%} > max {settings.WEATHER_MAX_ENTRY_PRICE:.0%}")
    if prob_result.low_confidence_flag:
        filter_notes.append(f"CDF/fraction disagree: {model_yes_prob:.0%} vs {prob_result.ensemble_fraction:.0%}")
    filter_note = f" [{', '.join(filter_notes)}]" if filter_notes else ""

    reasoning = (
        f"[{status}]{filter_note} "
        f"{market.city_name} {market.metric} {market.direction} {market.threshold_f:.0f}F on {market.target_date} | "
        f"Gaussian: mean={prob_result.ensemble_mean:.1f}F std={prob_result.ensemble_std:.1f}F "
        f"adj_std={prob_result.adjusted_std:.1f}F (x{prob_result.lead_time_factor}) | "
        f"Model YES: {model_yes_prob:.0%} | Raw fraction: {prob_result.ensemble_fraction:.0%} | "
        f"Market: {market_yes_prob:.0%} | Edge: {edge:+.1%} → {direction.upper()} @ {entry_price:.0%} | "
        f"Min edge after fees: {min_edge:.1%} | Confidence: {prob_result.confidence:.0%}"
    )

    return WeatherTradingSignal(
        market=market,
        model_probability=model_yes_prob,
        market_probability=market_yes_prob,
        edge=edge,
        direction=direction,
        confidence=prob_result.confidence,
        kelly_fraction=suggested_size / settings.INITIAL_BANKROLL if settings.INITIAL_BANKROLL > 0 else 0,
        suggested_size=suggested_size,
        reasoning=reasoning,
        ensemble_mean=prob_result.ensemble_mean,
        ensemble_std=prob_result.ensemble_std,
        ensemble_members=len(member_values),
        low_confidence_flag=prob_result.low_confidence_flag,
    )


async def scan_for_weather_signals() -> List[WeatherTradingSignal]:
    """Scan Kalshi weather markets and generate Gaussian CDF-based signals."""
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
        logger.info(
            f"  {s.market.city_name} {s.market.metric} {s.market.direction} "
            f"{s.market.threshold_f:.0f}F | Edge: {s.edge:+.1%} → {s.direction.upper()} "
            f"| Conf: {s.confidence:.0%}{'  ⚠ low-conf' if s.low_confidence_flag else ''}"
        )

    _persist_signals(signals)
    return signals


def _persist_signals(signals: List[WeatherTradingSignal]):
    """Save signals to DB for calibration tracking."""
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
                sources=["open_meteo_gfs_ensemble"],
                reasoning=signal.reasoning,
                executed=False,
            ))

        db.commit()
    except Exception as e:
        logger.warning(f"Failed to persist signals: {e}")
        db.rollback()
    finally:
        db.close()
