"""
Unit tests for the money math — pure, no network, no DB.

Covers the arithmetic that decides whether and how much real money is committed:
Kelly sizing, Kalshi fees / breakeven edge, the probability blend, the trade
threshold gate (incl. the item-3 negative-edge regression), settlement P&L, and
the warm/cold outlook helper shared by the paper and live position guards.
"""
import sys
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest
from scipy.stats import norm

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from weatherbot.config import settings
from weatherbot.core.paper_trading import is_warm_outlook
from weatherbot.core.probability import (
    LOW_CONFIDENCE_EDGE_OVERRIDE,
    compute_probability,
    kalshi_trade_fee,
    kalshi_trade_fee_ceil,
    kelly_size,
    min_profitable_edge,
    settlement_pnl,
)
from weatherbot.core.weather_signals import WeatherTradingSignal

ET = ZoneInfo("America/New_York")


# ── Kalshi fee / breakeven edge ──────────────────────────────────────────────
def test_kalshi_trade_fee_known_values():
    # fee = C · n · P · (1 − P)
    assert kalshi_trade_fee(0.5, 1, 0.07) == pytest.approx(0.0175)
    assert kalshi_trade_fee(0.5, 10, 0.07) == pytest.approx(0.175)
    assert kalshi_trade_fee(0.1, 1, 0.07) == pytest.approx(0.0063)
    # symmetric in P ↔ 1−P
    assert kalshi_trade_fee(0.9, 1, 0.07) == pytest.approx(kalshi_trade_fee(0.1, 1, 0.07))
    # cheapest near the extremes, dearest at 50¢
    assert kalshi_trade_fee(0.5, 1, 0.07) > kalshi_trade_fee(0.05, 1, 0.07)


def test_min_profitable_edge():
    assert min_profitable_edge(0.5, 0.07) == pytest.approx(0.0175)
    assert min_profitable_edge(0.1, 0.07) == pytest.approx(0.0063)
    assert min_profitable_edge(0.1, 0.07) == pytest.approx(min_profitable_edge(0.9, 0.07))
    # per-contract fee at P equals the min edge at P
    assert min_profitable_edge(0.3, 0.07) == pytest.approx(kalshi_trade_fee(0.3, 1, 0.07))


# ── Kelly sizing ─────────────────────────────────────────────────────────────
def test_kelly_size_known_value_yes():
    # model 0.70, entry 0.50, fee 0.07, kelly 0.15, bankroll 1000.
    # fee=0.0175; net_win=0.4825; net_loss=0.5175; b=0.932368;
    # f*=0.7 − 0.3/b = 0.378241; size = f*·0.15·1000 ≈ 56.74
    size = kelly_size(0.70, 0.50, "yes", 1000.0, 0.15, 0.07)
    assert size == pytest.approx(56.74, abs=0.05)


def test_kelly_size_yes_no_symmetry():
    # a NO bet at model 0.30 has the same p_win (0.70) as a YES bet at 0.70
    yes = kelly_size(0.70, 0.50, "yes", 1000.0, 0.15, 0.07)
    no  = kelly_size(0.30, 0.50, "no",  1000.0, 0.15, 0.07)
    assert no == pytest.approx(yes)


def test_kelly_size_zero_when_edge_negative():
    # p_win below entry → no Kelly edge → 0 (never a negative size)
    assert kelly_size(0.40, 0.50, "yes", 1000.0, 0.15, 0.07) == 0.0
    assert kelly_size(0.60, 0.50, "no",  1000.0, 0.15, 0.07) == 0.0


def test_kelly_size_scales_linearly():
    base = kelly_size(0.70, 0.50, "yes", 1000.0, 0.15, 0.07)
    assert kelly_size(0.70, 0.50, "yes", 1000.0, 0.30, 0.07) == pytest.approx(2 * base)
    assert kelly_size(0.70, 0.50, "yes", 2000.0, 0.15, 0.07) == pytest.approx(2 * base)


def test_kelly_size_rejects_degenerate_prices():
    assert kelly_size(0.70, 0.0, "yes", 1000.0, 0.15, 0.07) == 0.0
    assert kelly_size(0.70, 1.0, "yes", 1000.0, 0.15, 0.07) == 0.0


# ── Probability blend (70% ensemble fraction + 30% Gaussian CDF) ──────────────
# Fix the clock so the lead-time factor is a deterministic 1.0 (0–12h band).
_TARGET = date(2026, 7, 10)
_ASOF = datetime(2026, 7, 10, 12, 0, 0, tzinfo=ET)


def test_compute_probability_blend_arithmetic():
    members = [70.0, 72.0, 74.0, 76.0, 78.0]  # mean 74, std sqrt(10)=3.1623
    r = compute_probability(members, 75.0, "above", _TARGET, metric="high", as_of=_ASOF)
    assert r is not None
    assert r.ensemble_mean == pytest.approx(74.0)
    assert r.ensemble_std == pytest.approx(3.16227766)
    assert r.lead_time_factor == pytest.approx(1.0)
    frac = 2 / 5  # members above 75: {76, 78}
    assert r.ensemble_fraction == pytest.approx(frac)
    # adj_std 3.1623 > 3.0 floor, so the raw std drives the CDF here
    gcdf = 1.0 - norm.cdf(75.0, loc=74.0, scale=3.16227766)
    assert r.model_prob == pytest.approx(0.70 * frac + 0.30 * gcdf)


def test_compute_probability_std_floor_applied():
    # Ultra-tight ensemble (mean 74, raw std ≈ 0.035). Threshold 1°F above the
    # mean: with the 3°F floor the CDF gives ~0.37 (blend ~0.11); WITHOUT the
    # floor the CDF collapses to ~0 and the blend clamps to the 0.05 floor. The
    # unclamped ~0.11 result is only reachable when the std floor is applied.
    members = [73.95, 74.0, 74.05, 74.0, 74.0]
    r = compute_probability(members, 75.0, "above", _TARGET, metric="high", as_of=_ASOF)
    floored_cdf = 1.0 - norm.cdf(75.0, loc=74.0, scale=3.0)  # floor=3.0 used inside
    assert r.model_prob == pytest.approx(0.30 * floored_cdf)  # frac above 75 = 0
    assert r.model_prob > 0.08                                # not collapsed to the 0.05 clamp
    assert r.adjusted_std < 0.1                               # floor is CDF-only, not on adjusted_std


def test_compute_probability_clamps():
    high = compute_probability([90, 91, 92, 93, 94], 75.0, "above", _TARGET, as_of=_ASOF)
    low  = compute_probability([90, 91, 92, 93, 94], 75.0, "below", _TARGET, as_of=_ASOF)
    assert high.model_prob == pytest.approx(0.95)  # clamp ceiling
    assert low.model_prob == pytest.approx(0.05)   # clamp floor


def test_compute_probability_direction_symmetry():
    # No member equals the threshold → P(above) + P(below) == 1 (pre-clamp).
    members = [70.0, 72.0, 74.0, 76.0, 78.0]
    above = compute_probability(members, 75.0, "above", _TARGET, as_of=_ASOF)
    below = compute_probability(members, 75.0, "below", _TARGET, as_of=_ASOF)
    assert above.model_prob + below.model_prob == pytest.approx(1.0)


def test_compute_probability_insufficient_data():
    assert compute_probability([], 75.0, "above", _TARGET, as_of=_ASOF) is None
    assert compute_probability([74.0], 75.0, "above", _TARGET, as_of=_ASOF) is None


# ── Threshold gate (item-3 regression) ───────────────────────────────────────
def _sig(edge, agreement="HIGH", low_conf=False):
    return WeatherTradingSignal(market=None, edge=edge, agreement=agreement,
                                low_confidence_flag=low_conf)


def test_positive_edge_passes_negative_does_not():
    M = settings.MIN_EDGE_THRESHOLD
    assert _sig(M + 0.02).passes_threshold is True
    assert _sig(M + 0.02).passes_paper_threshold is True
    # same magnitude, negative → must NOT pass either (the abs() bug)
    assert _sig(-(M + 0.02)).passes_threshold is False
    assert _sig(-(M + 0.02)).passes_paper_threshold is False


def test_low_agreement_raises_bar_to_override():
    M, L = settings.MIN_EDGE_THRESHOLD, LOW_CONFIDENCE_EDGE_OVERRIDE
    assert M < L  # precondition for the gap to exist
    mid = (M + L) / 2  # clears the normal bar but not the LOW bar
    assert _sig(mid, agreement="HIGH").passes_threshold is True
    assert _sig(mid, agreement="LOW").passes_threshold is False
    assert _sig(mid, low_conf=True).passes_threshold is False
    assert _sig(L + 0.02, agreement="LOW").passes_threshold is True
    # paper threshold ignores agreement — only the raw MIN edge
    assert _sig(mid, agreement="LOW").passes_paper_threshold is True


# ── Settlement P&L ───────────────────────────────────────────────────────────
def test_kalshi_trade_fee_ceil():
    # raw 0.07·10·0.40·0.60 = 0.168 → ceil to next cent = 0.17
    assert kalshi_trade_fee_ceil(0.40, 10, 0.07) == pytest.approx(0.17)
    # raw 0.0175 → 0.02
    assert kalshi_trade_fee_ceil(0.50, 1, 0.07) == pytest.approx(0.02)
    # always ≥ the un-ceiled fee
    assert kalshi_trade_fee_ceil(0.37, 7, 0.07) >= kalshi_trade_fee(0.37, 7, 0.07)


def test_settlement_pnl_win_and_loss():
    # entry 0.40, 10 contracts. Kalshi ceils the fee to the next cent:
    # raw 0.168 → 0.17.
    win = settlement_pnl(0.40, 10, True, 0.07)
    loss = settlement_pnl(0.40, 10, False, 0.07)
    assert win == pytest.approx(0.60 * 10 - 0.17)     # 5.83
    assert loss == pytest.approx(-0.40 * 10 - 0.17)   # -4.17
    assert round(win, 2) == 5.83
    assert round(loss, 2) == -4.17
    # the fee is always subtracted, win or lose
    assert win < 0.60 * 10
    assert loss < -0.40 * 10


# ── Warm/cold outlook helper (shared paper + live guard) ─────────────────────
def test_is_warm_outlook_table():
    assert is_warm_outlook("above", "yes") is True   # bet it gets hot → warm
    assert is_warm_outlook("below", "no") is True    # bet it does NOT stay cool → warm
    assert is_warm_outlook("above", "no") is False    # cold
    assert is_warm_outlook("below", "yes") is False   # cold
