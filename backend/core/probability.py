"""
Gaussian CDF probability engine for weather temperature markets.

Replaces the raw ensemble fraction approach with a calibrated Gaussian model
that accounts for lead-time uncertainty.
"""
import logging
from dataclasses import dataclass
from datetime import datetime, date
from typing import List, Optional

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
