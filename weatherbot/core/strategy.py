"""
StrategyParams — every tunable of the trading strategy in one place.

This is the linchpin of the shared-engine design (see docs and the backtest
plan): the live bot and the backtester run the *same* decision code, differing
only in the ``StrategyParams`` they feed it and the data/clock they feed it from.

``StrategyParams.live_default()`` reproduces today's live behavior exactly —
settings-derived fields are read from the ``settings`` singleton (so the .env
config editor keeps working), and the model/filter constants mirror the values
currently hard-coded in ``probability.py`` and ``weather_signals.py``. Any drift
between this default and live behavior is caught by tests/test_engine_fidelity.py.

The dataclass is frozen: a strategy is an immutable description. The backtester
builds variants with ``dataclasses.replace(params, kelly_fraction=0.10, ...)``.
"""
from __future__ import annotations

from dataclasses import dataclass, field, replace
from typing import Dict, List, Tuple

from weatherbot.config import settings


# Lead-time uncertainty inflation: (lo_hours, hi_hours, factor). Mirrors
# probability.LEAD_TIME_FACTORS. Multiplied into the ensemble std before the CDF.
DEFAULT_LEAD_TIME_FACTORS: Tuple[Tuple[float, float, float], ...] = (
    (0, 12, 1.0),
    (12, 24, 1.1),
    (24, 48, 1.3),
    (48, 72, 1.5),
    (72, float("inf"), 1.8),
)

# Static per-source blend weights (mirrors probability.SOURCE_WEIGHTS).
DEFAULT_SOURCE_WEIGHTS: Dict[str, float] = {
    "nws":   0.30,
    "ecmwf": 0.30,
    "gfs":   0.25,
    "gem":   0.15,
}

# Model-run publish times in ET (hour, minute) — mirrors
# weather_signals.MODEL_RUN_HOURS_ET. Used for the staleness gate.
DEFAULT_MODEL_RUN_HOURS_ET: Tuple[Tuple[int, int], ...] = (
    (3, 30), (9, 30), (15, 30), (21, 30),
)


@dataclass(frozen=True)
class StrategyParams:
    """Immutable, fully-explicit description of one trading strategy."""

    # ── Probability blend (probability.py) ───────────────────────────────────
    ensemble_fraction_weight: float = 0.70
    gaussian_cdf_weight: float = 0.30
    std_floor_high: float = 3.0
    std_floor_low: float = 2.0
    lead_time_factors: Tuple[Tuple[float, float, float], ...] = DEFAULT_LEAD_TIME_FACTORS
    prob_floor: float = 0.05
    prob_ceiling: float = 0.95
    prob_floor_sizing: float = 0.15       # sizing substitute when prob hits floor
    prob_ceiling_sizing: float = 0.85     # sizing substitute when prob hits ceiling

    # ── Source blending / agreement (probability.py) ─────────────────────────
    source_weights: Dict[str, float] = field(default_factory=lambda: dict(DEFAULT_SOURCE_WEIGHTS))
    use_dynamic_brier_weights: bool = True
    agreement_tight: float = 0.10         # all sources within this → HIGH
    majority_band: float = 0.15           # 3-of-4 cluster within this → MEDIUM
    outlier_threshold: float = 0.40       # source this far from others' median → outlier
    outlier_dampen: float = 0.50          # fraction of an outlier's weight to redistribute

    # ── Filters (weather_signals.py) ─────────────────────────────────────────
    model_divergence_threshold: float = 8.0    # °F GFS vs ECMWF mean gap → skip
    climatology_deviation_max: float = 1.5      # °F within climo normal → no edge
    obs_window_hours: float = 6.0               # apply observation constraint within this
    cold_day_margin: float = 4.0                # °F below threshold for cold-day YES exception
    cold_day_nws_min: float = 0.85              # NWS prob floor for cold-day YES exception
    yes_entry_floor: float = 0.30               # min entry price for YES bets
    rain_entry_floor: float = 0.05              # min entry price for rain bets
    entry_min_price: float = 0.10               # WEATHER_MIN_ENTRY_PRICE
    entry_max_price: float = 0.70               # WEATHER_MAX_ENTRY_PRICE
    same_day_high_cutoff_hour: int = 9          # stop same-day high entries at/after (ET)
    same_day_low_cutoff_hour: int = 7           # stop same-day low entries at/after (ET)
    conviction_threshold: float = 0.75          # bypass time gates when model & market ≥ this
    model_run_hours_et: Tuple[Tuple[int, int], ...] = DEFAULT_MODEL_RUN_HOURS_ET
    model_data_max_age_hours: float = 5.0       # skip if last model run older than this

    # ── Sizing / bankroll ────────────────────────────────────────────────────
    kelly_fraction: float = 0.15
    min_edge_threshold: float = 0.08
    low_confidence_edge_override: float = 0.15  # higher edge bar for LOW-agreement signals
    max_trade_size: float = 100.0               # WEATHER_MAX_TRADE_SIZE
    bankroll_basis: str = "cash"                # "cash" | "equity"
    initial_bankroll: float = 1000.0

    # ── Execution / fills ─────────────────────────────────────────────────────
    fill_mode: str = "taker"                    # "taker" | "maker"
    maker_post_at: str = "mid"                  # "bid" | "mid" | "bid_plus"
    maker_offset: float = 0.0                   # ¢ (0-1) added to post price for bid_plus
    taker_fee_coef: float = 0.07                # Kalshi fee coefficient C in C·n·P·(1-P)
    maker_fee_coef: float = 0.0                 # maker fee coefficient (0 = free maker)

    # ── Liquidity filters ─────────────────────────────────────────────────────
    min_ask_size: int = 25
    min_volume_24h: int = 200

    # ── Identity ──────────────────────────────────────────────────────────────
    name: str = "live_default"

    @classmethod
    def live_default(cls) -> "StrategyParams":
        """
        The strategy that reproduces today's live behavior exactly.

        Settings-derived fields come from the ``settings`` singleton so the
        dashboard's .env config editor keeps driving the live bot. Model/filter
        constants mirror the current module-level values in probability.py and
        weather_signals.py.
        """
        return cls(
            kelly_fraction=settings.KELLY_FRACTION,
            min_edge_threshold=settings.MIN_EDGE_THRESHOLD,
            max_trade_size=settings.WEATHER_MAX_TRADE_SIZE,
            entry_min_price=settings.WEATHER_MIN_ENTRY_PRICE,
            entry_max_price=settings.WEATHER_MAX_ENTRY_PRICE,
            taker_fee_coef=settings.KALSHI_FEE_RATE,
            bankroll_basis="equity" if settings.KELLY_USE_EQUITY else "cash",
            initial_bankroll=settings.INITIAL_BANKROLL,
            same_day_high_cutoff_hour=settings.SAME_DAY_HIGH_CUTOFF_HOUR,
            same_day_low_cutoff_hour=settings.SAME_DAY_LOW_CUTOFF_HOUR,
            conviction_threshold=settings.TRADING_HOURS_CONVICTION_THRESHOLD,
            min_ask_size=settings.MIN_ASK_SIZE,
            min_volume_24h=settings.MIN_VOLUME_24H,
            name="live_default",
        )

    def with_(self, **overrides) -> "StrategyParams":
        """Return a copy with the given fields replaced (convenience over replace())."""
        return replace(self, **overrides)
