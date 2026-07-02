"""
Gaussian CDF probability engine — single and multi-source ensemble-of-ensembles.

Probability blending policy (Task 3 fix):
  - Ensemble fraction is the PRIMARY signal (fraction of members on YES side).
  - Gaussian CDF provides a smoothing correction only (30% weight).
  - A minimum std floor prevents over-confidence when the ensemble is tight:
      daily high: std >= STD_FLOOR_HIGH (3°F)
      daily low:  std >= STD_FLOOR_LOW  (2°F)
    (floor is applied BEFORE the CDF, not to the ensemble fraction itself)
  - Final blend: 70% ensemble_fraction + 30% gaussian_cdf, clamped [0.05, 0.95].
"""
import logging
import math
from dataclasses import dataclass, field
from datetime import datetime, date
from typing import Dict, List, Optional, TYPE_CHECKING

from scipy.stats import norm

if TYPE_CHECKING:
    from weatherbot.core.strategy import StrategyParams

logger = logging.getLogger("weatherbot")

# Lead-time uncertainty correction factors
# Multiplied by ensemble std to get adjusted (inflated) std
LEAD_TIME_FACTORS = [
    (0, 12, 1.0),    # 0-12 hours out
    (12, 24, 1.1),   # 12-24 hours
    (24, 48, 1.3),   # 24-48 hours
    (48, 72, 1.5),   # 48-72 hours
    (72, float("inf"), 1.8),  # 72+ hours
]

# Minimum std floors to prevent over-confidence when ensemble is tight (°F)
STD_FLOOR_HIGH = 3.0   # daily high markets
STD_FLOOR_LOW  = 2.0   # daily low markets

# Blend weights: ensemble fraction vs Gaussian CDF
ENSEMBLE_FRACTION_WEIGHT = 0.70
GAUSSIAN_CDF_WEIGHT      = 0.30


def _lead_time_factor(
    target_date: date,
    as_of: Optional[datetime] = None,
    factors: Optional[list] = None,
) -> float:
    """
    Compute uncertainty inflation factor based on hours until market resolution.

    ``as_of`` injects the decision-time clock (a tz-aware datetime). When None,
    the live wall clock is used — preserving the original behavior. ``factors``
    overrides the lead-time bands (defaults to the module ``LEAD_TIME_FACTORS``).
    """
    from zoneinfo import ZoneInfo
    ET = ZoneInfo("America/New_York")
    now = as_of.astimezone(ET) if as_of is not None else datetime.now(ET)
    # Kalshi temperature markets settle at end of day ET (11:59 PM ET).
    resolution_dt = datetime(target_date.year, target_date.month, target_date.day, 23, 59, 59, tzinfo=ET)
    hours_out = max(0.0, (resolution_dt - now).total_seconds() / 3600.0)

    bands = factors if factors is not None else LEAD_TIME_FACTORS
    for lo, hi, factor in bands:
        if lo <= hours_out < hi:
            return factor
    return bands[-1][2]  # fallback for beyond the last band


@dataclass
class ProbabilityResult:
    """Result from the Gaussian CDF probability calculation."""
    model_prob: float        # P(YES) from Gaussian CDF
    ensemble_fraction: float # Raw fraction of ensemble members on YES side
    ensemble_mean: float
    ensemble_std: float
    adjusted_std: float      # After lead-time inflation
    lead_time_factor: float
    confidence: float        # 1 - (adjusted_std / 10), clamped [0.3, 0.95]
    low_confidence_flag: bool  # True if CDF and fraction disagree by >15%


def compute_probability(
    member_values: List[float],
    threshold_f: float,
    direction: str,           # "above" or "below"
    target_date: date,
    metric: str = "high",     # "high" or "low" — used to pick the right std floor
    params: Optional["StrategyParams"] = None,
    as_of: Optional[datetime] = None,
) -> Optional[ProbabilityResult]:
    """
    Compute calibrated probability that temperature is above/below threshold.

    Uses ensemble fraction as the primary signal (70%) with Gaussian CDF as a
    smoothing correction (30%).  A minimum std floor (3°F for highs, 2°F for lows)
    is applied before the CDF to prevent extreme probabilities when the ensemble
    is artificially tight.

    Args:
        member_values: List of ensemble member temperature values (Fahrenheit)
        threshold_f: Temperature threshold to compare against
        direction: "above" (P(temp > threshold)) or "below" (P(temp < threshold))
        target_date: The date the market resolves
        metric: "high" or "low" — selects STD_FLOOR_HIGH vs STD_FLOOR_LOW

    Returns:
        ProbabilityResult or None if insufficient data
    """
    if not member_values or len(member_values) < 2:
        return None

    # Resolve tunables from params, falling back to module constants when no
    # StrategyParams is supplied (preserves the original call sites).
    ef_weight   = params.ensemble_fraction_weight if params else ENSEMBLE_FRACTION_WEIGHT
    cdf_weight  = params.gaussian_cdf_weight       if params else GAUSSIAN_CDF_WEIGHT
    floor_high  = params.std_floor_high            if params else STD_FLOOR_HIGH
    floor_low   = params.std_floor_low             if params else STD_FLOOR_LOW
    lt_factors  = list(params.lead_time_factors)   if params else None
    prob_lo     = params.prob_floor                if params else 0.05
    prob_hi     = params.prob_ceiling              if params else 0.95

    import statistics
    mean = statistics.mean(member_values)
    std = statistics.stdev(member_values)

    if std <= 0:
        std = 0.1  # Avoid division by zero; degenerate case

    factor = _lead_time_factor(target_date, as_of=as_of, factors=lt_factors)
    adj_std = std * factor

    # Apply std floor BEFORE the CDF to prevent over-extrapolation.
    # The floor is NOT applied to the ensemble fraction (that stays raw).
    std_floor = floor_high if metric != "low" else floor_low
    cdf_std = max(adj_std, std_floor)

    # Raw ensemble fraction — PRIMARY signal
    if direction == "above":
        ensemble_fraction = sum(1 for v in member_values if v > threshold_f) / len(member_values)
        gaussian_cdf = float(1.0 - norm.cdf(threshold_f, loc=mean, scale=cdf_std))
    else:
        ensemble_fraction = sum(1 for v in member_values if v < threshold_f) / len(member_values)
        gaussian_cdf = float(norm.cdf(threshold_f, loc=mean, scale=cdf_std))

    # Blend: ensemble fraction + Gaussian CDF (default 70/30)
    model_prob = ef_weight * ensemble_fraction + cdf_weight * gaussian_cdf

    # Clamp to avoid extreme values
    model_prob = max(prob_lo, min(prob_hi, model_prob))

    # Confidence: lower std = higher confidence
    confidence = 1.0 - (adj_std / 10.0)
    confidence = max(0.3, min(0.95, confidence))

    # Flag if blended prob and raw fraction disagree by >15%
    low_confidence_flag = abs(model_prob - ensemble_fraction) > 0.15

    return ProbabilityResult(
        model_prob=model_prob,
        ensemble_fraction=ensemble_fraction,
        ensemble_mean=mean,
        ensemble_std=std,
        adjusted_std=adj_std,
        lead_time_factor=factor,
        confidence=confidence,
        low_confidence_flag=low_confidence_flag,
    )


# ── Source weights for ensemble-of-ensembles ─────────────────────────────────
SOURCE_WEIGHTS: Dict[str, float] = {
    "nws":   0.30,   # Closest to Kalshi's resolution source (NOAA obs)
    "ecmwf": 0.30,   # Generally most accurate global model
    "gfs":   0.25,   # Solid baseline
    "gem":   0.15,   # Additional independent signal
}

# Agreement thresholds
AGREEMENT_TIGHT    = 0.10   # all sources within 10% → HIGH
MAJORITY_BAND      = 0.15   # 3 of 4 within 15% of each other → MEDIUM (not LOW)
OUTLIER_THRESHOLD  = 0.40   # source >40% from the other 3's median → outlier
OUTLIER_DAMPEN     = 0.50   # reduce outlier weight by this fraction

# Edge threshold override when models genuinely split 2v2
LOW_CONFIDENCE_EDGE_OVERRIDE = 0.15


@dataclass
class SourceProbability:
    """Probability estimate from a single source."""
    source: str
    prob: float          # P(YES) from this source
    members: int
    mean: float
    std: float
    ok: bool = True


@dataclass
class MultiSourceResult:
    """Combined probability from the ensemble-of-ensembles."""
    combined_prob: float                           # weighted average P(YES)
    source_probs: Dict[str, SourceProbability]     # per-source breakdown
    agreement: str                                  # "HIGH", "MEDIUM", "LOW"
    max_spread: float                               # max pairwise probability spread
    weights_used: Dict[str, float]                  # normalised weights actually applied (after outlier dampening)
    low_confidence_flag: bool                       # True if agreement == LOW (genuine 2v2 split)
    outlier_dampened: Optional[str] = None          # source name if outlier dampening was applied
    # For backwards compat with single-source pipeline
    ensemble_mean: float = 0.0
    ensemble_std: float  = 0.0
    ensemble_fraction: float = 0.0
    adjusted_std: float  = 0.0
    lead_time_factor: float = 1.0
    confidence: float    = 0.7


# ── Dynamic source weights from per-city Brier scores ────────────────────────
# Cache: (city_key, metric) -> (timestamp, weights_dict)
_dynamic_weight_cache: Dict[str, tuple] = {}
_WEIGHT_CACHE_TTL = 3600.0   # refresh every 1 hour


def get_dynamic_source_weights(city_key: str, metric: str) -> Dict[str, float]:
    """
    Query ModelCityAccuracy for per-model Brier scores and compute
    inverse-Brier weights. Falls back to static SOURCE_WEIGHTS when
    insufficient data (n < 10 for any source).

    Weights are cached per (city_key, metric) for 1 hour to avoid
    repeated DB queries on every signal generation call.
    """
    import time as _time
    cache_key = f"{city_key}:{metric}"
    now = _time.time()

    if cache_key in _dynamic_weight_cache:
        cached_at, cached_weights = _dynamic_weight_cache[cache_key]
        if now - cached_at < _WEIGHT_CACHE_TTL:
            return cached_weights

    try:
        from weatherbot.models.trade import SessionLocal as PaperSessionLocal, ModelCityAccuracy
        db = PaperSessionLocal()
        try:
            rows = (
                db.query(ModelCityAccuracy)
                .filter(
                    ModelCityAccuracy.city  == city_key,
                    ModelCityAccuracy.metric == metric,
                    ModelCityAccuracy.n     >= 10,
                )
                .all()
            )
        finally:
            db.close()

        if not rows:
            logger.debug(
                f"Dynamic weights: insufficient data for {city_key}/{metric} — using static weights"
            )
            _dynamic_weight_cache[cache_key] = (now, SOURCE_WEIGHTS)
            return SOURCE_WEIGHTS

        # Inverse-Brier weight: lower Brier score = higher weight
        raw: Dict[str, float] = {}
        for row in rows:
            if row.n > 0 and row.brier_sum >= 0:
                brier = row.brier_sum / row.n
                raw[row.model] = 1.0 / max(brier, 1e-6)   # avoid div/0
            else:
                raw[row.model] = SOURCE_WEIGHTS.get(row.model, 0.20)

        # Only use models present in static weights; fall back for missing ones
        weights: Dict[str, float] = {}
        for model in SOURCE_WEIGHTS:
            weights[model] = raw.get(model, SOURCE_WEIGHTS[model])

        total = sum(weights.values())
        if total <= 0:
            return SOURCE_WEIGHTS
        normalized = {k: v / total for k, v in weights.items()}

        logger.debug(
            f"Dynamic weights {city_key}/{metric}: "
            + "  ".join(f"{k}={v:.3f}" for k, v in normalized.items())
        )
        _dynamic_weight_cache[cache_key] = (now, normalized)
        return normalized

    except Exception as e:
        logger.debug(f"Dynamic weight lookup failed for {city_key}/{metric}: {e}")
        return SOURCE_WEIGHTS


def compute_multi_source_probability(
    sources: Dict,            # Dict[str, SourceForecast] from multi_source_weather
    threshold_f: float,
    direction: str,           # "above" or "below"
    target_date: date,
    metric: str = "high",     # "high" or "low" — selects std floor
    city_key: str = "",       # used for dynamic weight lookup (optional)
    params: Optional["StrategyParams"] = None,
    as_of: Optional[datetime] = None,
) -> Optional[MultiSourceResult]:
    """
    Compute ensemble-of-ensembles probability from multiple weather sources.

    Each source's member array is fed through the Gaussian CDF independently.
    Results are combined with the configured source weights (renormalised if
    sources are missing). Cross-model agreement is assessed to set the
    confidence tier.

    ``params`` supplies all tunables (weights, std floors, agreement bands,
    outlier dampening, blend weights, clamps); when None the module constants
    are used. ``as_of`` injects the decision-time clock for lead-time inflation.
    """
    # Resolve tunables (params override module constants).
    ef_weight    = params.ensemble_fraction_weight if params else ENSEMBLE_FRACTION_WEIGHT
    cdf_weight   = params.gaussian_cdf_weight       if params else GAUSSIAN_CDF_WEIGHT
    floor_high   = params.std_floor_high            if params else STD_FLOOR_HIGH
    floor_low    = params.std_floor_low             if params else STD_FLOOR_LOW
    lt_factors   = list(params.lead_time_factors)   if params else None
    prob_lo      = params.prob_floor                if params else 0.05
    prob_hi      = params.prob_ceiling              if params else 0.95
    agree_tight  = params.agreement_tight           if params else AGREEMENT_TIGHT
    maj_band     = params.majority_band             if params else MAJORITY_BAND
    outlier_thr  = params.outlier_threshold         if params else OUTLIER_THRESHOLD
    outlier_damp = params.outlier_dampen            if params else OUTLIER_DAMPEN

    source_probs: Dict[str, SourceProbability] = {}
    factor = _lead_time_factor(target_date, as_of=as_of, factors=lt_factors)

    for name, src in sources.items():
        if not src.ok or not src.member_highs:
            continue

        import statistics as _stats
        members = src.member_highs  # caller pre-loads the correct metric into member_highs — see weather_signals.py

        if len(members) < 1:
            continue

        mean = _stats.mean(members)
        std  = _stats.stdev(members) if len(members) > 1 else 0.0
        if std < 0:
            std = 0.0
        adj_std = std * factor

        # Apply std floor before CDF (same policy as single-source compute_probability)
        std_floor = floor_high if metric != "low" else floor_low
        cdf_std = max(adj_std, std_floor)

        if direction == "above":
            gaussian_cdf = float(1.0 - norm.cdf(threshold_f, loc=mean, scale=cdf_std))
        else:
            gaussian_cdf = float(norm.cdf(threshold_f, loc=mean, scale=cdf_std))

        if len(members) == 1:
            # Single deterministic value (e.g. NWS point forecast) — use pure CDF.
            # Ensemble fraction is meaningless (0 or 1) with one member.
            prob = gaussian_cdf
        else:
            # Raw ensemble fraction — PRIMARY signal
            if direction == "above":
                ensemble_frac = sum(1 for v in members if v > threshold_f) / len(members)
            else:
                ensemble_frac = sum(1 for v in members if v < threshold_f) / len(members)

            # Blend: ensemble fraction + Gaussian CDF (default 70/30)
            prob = ef_weight * ensemble_frac + cdf_weight * gaussian_cdf

        prob = max(prob_lo, min(prob_hi, prob))

        source_probs[name] = SourceProbability(
            source=name, prob=prob, members=len(members),
            mean=mean, std=adj_std, ok=True,
        )

    if not source_probs:
        return None

    names  = list(source_probs.keys())
    probs  = [source_probs[k].prob for k in names]
    max_spread = max(probs) - min(probs)

    # ── Outlier detection and weight dampening ────────────────────────────────
    # If one source is >OUTLIER_THRESHOLD away from the median of the other 3,
    # cut its weight by OUTLIER_DAMPEN and redistribute to the agreeing models.
    outlier_dampened: Optional[str] = None
    # Static base weights: params.source_weights when supplied, else module default.
    _static_weights = params.source_weights if params else SOURCE_WEIGHTS
    # Use dynamic per-city Brier weights when enabled and a city is given,
    # otherwise fall back to the static weights.
    _use_dynamic = params.use_dynamic_brier_weights if params else True
    if city_key and _use_dynamic:
        _base_weights = get_dynamic_source_weights(city_key, metric)
    else:
        _base_weights = _static_weights
    raw_weights = {k: _base_weights.get(k, 1.0 / len(names)) for k in names}

    if len(names) >= 3:
        import statistics as _stat
        for name in names:
            others = [source_probs[k].prob for k in names if k != name]
            others_median = _stat.median(others)
            if abs(source_probs[name].prob - others_median) >= outlier_thr:
                # This source is a clear outlier — dampen its weight
                saved = raw_weights[name] * outlier_damp
                raw_weights[name] -= saved
                # Redistribute evenly to the other (agreeing) sources
                per_other = saved / len(others)
                for k in names:
                    if k != name:
                        raw_weights[k] += per_other
                outlier_dampened = name
                logger.debug(
                    f"Outlier dampening: {name} prob={source_probs[name].prob:.0%} "
                    f"vs others median={others_median:.0%} "
                    f"— weight reduced {SOURCE_WEIGHTS.get(name,0):.2f}→{raw_weights[name]:.2f}"
                )
                break   # at most one outlier per signal

    total_w = sum(raw_weights.values())
    norm_weights = {k: v / total_w for k, v in raw_weights.items()}

    combined = sum(source_probs[k].prob * norm_weights[k] for k in names)
    combined = max(prob_lo, min(prob_hi, combined))

    # ── Agreement assessment (majority-rules) ─────────────────────────────────
    # HIGH:   all sources within AGREEMENT_TIGHT (10%)
    # MEDIUM: 3 of 4 within MAJORITY_BAND (15%) of each other
    # LOW:    genuine 2v2 split — no 3-source cluster within MAJORITY_BAND

    if max_spread <= agree_tight:
        agreement = "HIGH"
    elif len(names) < 3:
        agreement = "MEDIUM"
    else:
        # Check for 3-source majority cluster within the majority band
        sorted_probs = sorted(probs)
        majority_found = False
        if len(sorted_probs) >= 3:
            for i in range(len(sorted_probs) - 2):
                if sorted_probs[i + 2] - sorted_probs[i] <= maj_band:
                    majority_found = True
                    break
        agreement = "MEDIUM" if majority_found else "LOW"

    # LOW only means genuine 2v2 split (or worse) — not just one outlier
    low_confidence_flag = (agreement == "LOW")

    ref = source_probs.get("gfs") or next(iter(source_probs.values()))
    confidence = max(0.3, min(0.95, 1.0 - (ref.std / 10.0)))
    if agreement == "HIGH":
        confidence = min(0.95, confidence + 0.05)
    elif agreement == "LOW":
        confidence = max(0.3, confidence - 0.15)

    return MultiSourceResult(
        combined_prob=combined,
        source_probs=source_probs,
        agreement=agreement,
        max_spread=max_spread,
        weights_used=norm_weights,
        low_confidence_flag=low_confidence_flag,
        outlier_dampened=outlier_dampened,
        ensemble_mean=ref.mean,
        ensemble_std=ref.std,
        ensemble_fraction=combined,
        adjusted_std=ref.std,
        lead_time_factor=factor,
        confidence=confidence,
    )


def kalshi_trade_fee(entry_price: float, n_contracts: int = 1, fee_coef: float = 0.07) -> float:
    """
    Kalshi trading fee: C × n × P × (1 − P).
    Charged at execution on both sides, win or lose.
    Verify the current coefficient against Kalshi's fee schedule before deploying.
    """
    return fee_coef * n_contracts * entry_price * (1.0 - entry_price)


def kalshi_trade_fee_ceil(entry_price: float, n_contracts: int = 1, fee_coef: float = 0.07) -> float:
    """Kalshi rounds the trading fee UP to the next whole cent. Use this ceiled
    value where the exact charge matters — the balance preflight and settled
    P&L — so we never under-reserve cash or overstate realized P&L. (Kelly
    sizing keeps the smooth kalshi_trade_fee; a sub-cent difference there is
    immaterial and ceiling it would add noise.)"""
    return math.ceil(kalshi_trade_fee(entry_price, n_contracts, fee_coef) * 100.0) / 100.0


def settlement_pnl(entry_price: float, contracts: int, we_win: bool,
                   fee_rate: float = 0.07) -> float:
    """Realized P&L for a settled Kalshi position (raw, caller rounds).

    The fee C×n×P×(1−P) is charged at execution regardless of outcome:
      win:  (1 − P)×n − fee   (collect the other side's dollar, minus fee)
      loss: −P×n − fee        (lose the stake, still pay the fee)

    Shared by live and paper settlement so the two P&L formulas can't drift.
    The fee is ceiled to the next cent to match how Kalshi actually charges.
    """
    fee = kalshi_trade_fee_ceil(entry_price, contracts, fee_coef=fee_rate)
    if we_win:
        return (1.0 - entry_price) * contracts - fee
    return -entry_price * contracts - fee


def min_profitable_edge(entry_price: float, fee_coef: float = 0.07) -> float:
    """
    Minimum model edge to break even after Kalshi fees.

    Breakeven: model_prob = entry_price + fee_per_contract
             = entry_price + C × entry_price × (1 − entry_price)
    Min edge  = C × P × (1 − P)

    This is price-dependent — cheapest near the extremes, highest at 50¢.
    """
    return fee_coef * entry_price * (1.0 - entry_price)


def kelly_size(
    model_prob: float,
    entry_price: float,    # actual ask for the chosen direction (yes_price or no_price)
    direction: str,        # "yes" or "no"
    bankroll: float,
    kelly_fraction: float,
    fee_rate: float,
) -> float:
    """
    Kelly-sized position amount using the real Kalshi fee structure.

    Fee C×P×(1−P) is charged at execution on both winners and losers.
    Per contract:
      net win  = (1 − P) − fee
      net loss = P + fee
    Kelly f* = (p_win × b − (1 − p_win)) / b  where b = net_win / net_loss
    """
    p_win = model_prob if direction == "yes" else 1.0 - model_prob

    if entry_price <= 0 or entry_price >= 1:
        return 0.0

    fee = kalshi_trade_fee(entry_price, n_contracts=1, fee_coef=fee_rate)
    net_win  = (1.0 - entry_price) - fee
    net_loss = entry_price + fee

    if net_win <= 0 or net_loss <= 0:
        return 0.0

    b = net_win / net_loss
    kelly_f = (p_win * b - (1.0 - p_win)) / b
    kelly_f = max(0.0, kelly_f)

    return kelly_f * kelly_fraction * bankroll
