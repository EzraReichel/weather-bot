"""
Gaussian CDF probability engine — single and multi-source ensemble-of-ensembles.
"""
import logging
from dataclasses import dataclass, field
from datetime import datetime, date
from typing import Dict, List, Optional

from scipy.stats import norm

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


def _lead_time_factor(target_date: date) -> float:
    """Compute uncertainty inflation factor based on hours until market resolution."""
    now = datetime.utcnow()
    # Resolution is end of target_date (midnight UTC next day)
    resolution_dt = datetime(target_date.year, target_date.month, target_date.day, 23, 59, 59)
    hours_out = max(0.0, (resolution_dt - now).total_seconds() / 3600.0)

    for lo, hi, factor in LEAD_TIME_FACTORS:
        if lo <= hours_out < hi:
            return factor
    return 1.8  # fallback for beyond 72h


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
) -> Optional[ProbabilityResult]:
    """
    Compute calibrated probability that temperature is above/below threshold.

    Args:
        member_values: List of ensemble member temperature values (Fahrenheit)
        threshold_f: Temperature threshold to compare against
        direction: "above" (P(temp > threshold)) or "below" (P(temp < threshold))
        target_date: The date the market resolves

    Returns:
        ProbabilityResult or None if insufficient data
    """
    if not member_values or len(member_values) < 2:
        return None

    import statistics
    mean = statistics.mean(member_values)
    std = statistics.stdev(member_values)

    if std <= 0:
        std = 0.1  # Avoid division by zero; degenerate case

    factor = _lead_time_factor(target_date)
    adj_std = std * factor

    # Gaussian CDF probability
    if direction == "above":
        # P(temp > threshold) = 1 - CDF(threshold, mean, adj_std)
        model_prob = float(1.0 - norm.cdf(threshold_f, loc=mean, scale=adj_std))
        # Raw ensemble fraction
        ensemble_fraction = sum(1 for v in member_values if v > threshold_f) / len(member_values)
    else:
        # P(temp < threshold) = CDF(threshold, mean, adj_std)
        model_prob = float(norm.cdf(threshold_f, loc=mean, scale=adj_std))
        ensemble_fraction = sum(1 for v in member_values if v < threshold_f) / len(member_values)

    # Clamp to avoid extreme values
    model_prob = max(0.05, min(0.95, model_prob))

    # Confidence: lower std = higher confidence
    confidence = 1.0 - (adj_std / 10.0)
    confidence = max(0.3, min(0.95, confidence))

    # Flag if Gaussian CDF and raw fraction disagree by >15%
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

# Maximum probability spread (absolute) within which sources "agree"
AGREEMENT_TIGHT = 0.10   # all within 10% → HIGH
AGREEMENT_LOOSE = 0.20   # any pair > 20% → LOW

# Edge threshold override when models disagree badly
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
    weights_used: Dict[str, float]                  # normalised weights actually applied
    low_confidence_flag: bool                       # True if agreement == LOW
    # For backwards compat with single-source pipeline
    ensemble_mean: float = 0.0
    ensemble_std: float  = 0.0
    ensemble_fraction: float = 0.0
    adjusted_std: float  = 0.0
    lead_time_factor: float = 1.0
    confidence: float    = 0.7


def compute_multi_source_probability(
    sources: Dict,            # Dict[str, SourceForecast] from multi_source_weather
    threshold_f: float,
    direction: str,           # "above" or "below"
    target_date: date,
) -> Optional[MultiSourceResult]:
    """
    Compute ensemble-of-ensembles probability from multiple weather sources.

    Each source's member array is fed through the Gaussian CDF independently.
    Results are combined with SOURCE_WEIGHTS (renormalised if sources are missing).
    Cross-model agreement is assessed to set confidence tier.
    """
    source_probs: Dict[str, SourceProbability] = {}
    factor = _lead_time_factor(target_date)

    for name, src in sources.items():
        if not src.ok or not src.member_highs:
            continue

        import statistics as _stats
        members = src.member_highs if direction in ("above", "below") else src.member_lows
        # Pick the right list based on direction — but we don't know metric here,
        # so callers pass the already-correct list. Use member_highs as primary,
        # caller should pass appropriately filtered SourceForecast.
        members = src.member_highs  # caller is responsible — see weather_signals.py

        if len(members) < 2:
            continue

        mean = _stats.mean(members)
        std  = _stats.stdev(members)
        if std <= 0:
            std = 0.1
        adj_std = std * factor

        if direction == "above":
            prob = float(1.0 - norm.cdf(threshold_f, loc=mean, scale=adj_std))
        else:
            prob = float(norm.cdf(threshold_f, loc=mean, scale=adj_std))

        prob = max(0.05, min(0.95, prob))

        source_probs[name] = SourceProbability(
            source=name, prob=prob, members=len(members),
            mean=mean, std=adj_std, ok=True,
        )

    if not source_probs:
        return None

    # Normalise weights to available sources
    raw_weights = {k: SOURCE_WEIGHTS.get(k, 0.0) for k in source_probs}
    total_w = sum(raw_weights.values())
    if total_w <= 0:
        total_w = len(source_probs)
        raw_weights = {k: 1.0 for k in source_probs}
    norm_weights = {k: v / total_w for k, v in raw_weights.items()}

    combined = sum(source_probs[k].prob * norm_weights[k] for k in source_probs)
    combined = max(0.05, min(0.95, combined))

    # Agreement assessment
    probs = [sp.prob for sp in source_probs.values()]
    max_spread = max(probs) - min(probs)

    if len(probs) >= 4 and max_spread <= AGREEMENT_TIGHT:
        agreement = "HIGH"
    elif max_spread >= AGREEMENT_LOOSE:
        agreement = "LOW"
    else:
        # Check if at least 3/4 are within tight band
        sorted_probs = sorted(probs)
        tight_windows = [
            sorted_probs[i+2] - sorted_probs[i]
            for i in range(len(sorted_probs) - 2)
        ]
        if tight_windows and min(tight_windows) <= AGREEMENT_TIGHT:
            agreement = "MEDIUM"
        else:
            agreement = "MEDIUM"  # default to MEDIUM for 2-3 sources

    low_confidence_flag = (agreement == "LOW")

    # Backwards-compat fields: use GFS as reference for mean/std, else first available
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
        ensemble_mean=ref.mean,
        ensemble_std=ref.std,
        ensemble_fraction=combined,    # approximation
        adjusted_std=ref.std,
        lead_time_factor=factor,
        confidence=confidence,
    )


def min_profitable_edge(fee_rate: float) -> float:
    """
    Minimum edge needed to profit after Kalshi fees.
    fee_rate = fraction of profit taken as fee (e.g. 0.07 = 7%)
    min_edge = fee_rate / (1 - fee_rate)
    """
    return fee_rate / (1.0 - fee_rate)


def kelly_size(
    model_prob: float,
    market_price: float,
    direction: str,        # "yes" or "no"
    bankroll: float,
    kelly_fraction: float,
    fee_rate: float,
) -> float:
    """
    Kelly-sized position amount, net of Kalshi fees.

    For YES bet: entry_price = market_price (yes_price)
    For NO bet:  entry_price = 1 - market_price (no_price)

    Kelly fraction: f = (b*p - q) / b
    where b = (1 - entry_price) / entry_price (net odds)
          p = model_prob of winning
          q = 1 - p
    Then subtract fee from expected value before sizing.
    """
    if direction == "yes":
        entry = market_price
        p_win = model_prob
    else:
        entry = 1.0 - market_price
        p_win = 1.0 - model_prob

    if entry <= 0 or entry >= 1:
        return 0.0

    # Net odds per dollar risked
    b = (1.0 - entry) / entry

    # Fee-adjusted net odds: win pays b * (1 - fee_rate)
    b_net = b * (1.0 - fee_rate)

    q_win = 1.0 - p_win
    kelly_f = (b_net * p_win - q_win) / b_net if b_net > 0 else 0.0
    kelly_f = max(0.0, kelly_f)

    return kelly_f * kelly_fraction * bankroll
