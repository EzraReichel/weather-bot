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

import json
from dataclasses import dataclass, field, replace
from datetime import date
from functools import lru_cache
from pathlib import Path
from typing import Dict, List, Tuple

from weatherbot.config import settings


# Meteorological 3-month season buckets. Used to key season-aware calibration
# (per-city grid bias, std floors, blend weights). Three-month buckets are coarse
# enough that each accumulates a usable sample before it's trusted — month buckets
# are too sparse against the min_samples floor the calibration uses.
_SEASON_BY_MONTH: Dict[int, str] = {
    12: "DJF", 1: "DJF", 2: "DJF",
    3: "MAM", 4: "MAM", 5: "MAM",
    6: "JJA", 7: "JJA", 8: "JJA",
    9: "SON", 10: "SON", 11: "SON",
}


def season_for(d: date) -> str:
    """Meteorological season bucket for a date: 'DJF' | 'MAM' | 'JJA' | 'SON'."""
    return _SEASON_BY_MONTH[d.month]


# Optional file of per-season std-floor/blend overrides, written by
# scripts/apply_seasonal_calibration.py. Absent until calibration is applied, so
# live_default() gets {} and stays byte-for-byte identical to today.
_SEASONAL_OVERRIDES_PATH = Path(__file__).resolve().parent.parent / "seasonal_overrides.json"


@lru_cache(maxsize=1)
def _load_seasonal_overrides() -> Tuple[Tuple[str, Tuple[Tuple[str, float], ...]], ...]:
    """Load and freeze the seasonal-override config (cached). Returns a hashable
    tuple form so the cache is safe; callers rebuild a fresh dict from it."""
    try:
        raw = json.loads(_SEASONAL_OVERRIDES_PATH.read_text())
    except (FileNotFoundError, ValueError):
        return ()
    if not isinstance(raw, dict):
        return ()
    frozen = []
    for season, ov in raw.items():
        if isinstance(ov, dict):
            frozen.append((season, tuple((k, float(v)) for k, v in ov.items()
                                         if k in _SEASONAL_OVERRIDE_FIELDS)))
    return tuple(frozen)


def load_seasonal_overrides() -> Dict[str, Dict[str, float]]:
    """Fresh dict of {season: {field: value}} from the override config (or {})."""
    return {season: dict(ov) for season, ov in _load_seasonal_overrides()}


# Fields that may be overridden per-season (see StrategyParams.seasonal_overrides).
# Deliberately narrow: only the model-math knobs that calibration fits seasonally.
# Source weights, filters, and sizing are intentionally NOT season-aware.
_SEASONAL_OVERRIDE_FIELDS = frozenset({
    "std_floor_high", "std_floor_low",
    "ensemble_fraction_weight", "gaussian_cdf_weight",
})


# Lead-time uncertainty inflation: (lo_hours, hi_hours, factor). Mirrors
# probability.LEAD_TIME_FACTORS. Multiplied into the ensemble std before the CDF.
# The final band's upper bound is a large finite number rather than float('inf')
# so StrategyParams serializes to JSON (Postgres rejects Infinity); hours-to-
# resolution never approaches it, so behavior is identical.
LEAD_TIME_UPPER = 1_000_000.0
DEFAULT_LEAD_TIME_FACTORS: Tuple[Tuple[float, float, float], ...] = (
    (0, 12, 1.0),
    (12, 24, 1.1),
    (24, 48, 1.3),
    (48, 72, 1.5),
    (72, LEAD_TIME_UPPER, 1.8),
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

    # ── Season-aware calibration ──────────────────────────────────────────────
    # Optional per-season overrides of the model-math knobs in
    # _SEASONAL_OVERRIDE_FIELDS, keyed by season bucket ("DJF"/"MAM"/"JJA"/"SON").
    # Empty by default → for_season() is identity → behavior is unchanged. Only
    # seasons with enough calibration data are populated; absent seasons fall back
    # to the top-level (global) values. See for_season().
    seasonal_overrides: Dict[str, Dict[str, float]] = field(default_factory=dict)

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
            seasonal_overrides=load_seasonal_overrides(),
            name="live_default",
        )

    def with_(self, **overrides) -> "StrategyParams":
        """Return a copy with the given fields replaced (convenience over replace())."""
        return replace(self, **overrides)

    def for_season(self, target_date: date) -> "StrategyParams":
        """
        Resolve season-aware overrides for ``target_date``.

        Returns ``self`` unchanged when there are no overrides for the date's
        season (the common case, and the default) — so with an empty
        ``seasonal_overrides`` this is a true identity and live behavior is
        byte-for-byte unchanged. When a season bucket is present, returns a copy
        with only the whitelisted model-math fields replaced; all other fields
        (filters, sizing, source weights) are untouched.
        """
        if not self.seasonal_overrides:
            return self
        ov = self.seasonal_overrides.get(season_for(target_date))
        if not ov:
            return self
        applied = {k: v for k, v in ov.items() if k in _SEASONAL_OVERRIDE_FIELDS}
        if not applied:
            return self
        return replace(self, **applied)

    def to_dict(self) -> dict:
        """JSON-safe dict (tuples become lists, non-finite floats clamped). Round-trips via from_dict."""
        import math
        from dataclasses import asdict

        def _clean(v):
            if isinstance(v, float) and not math.isfinite(v):
                return LEAD_TIME_UPPER if v > 0 else -LEAD_TIME_UPPER
            if isinstance(v, dict):
                return {k: _clean(x) for k, x in v.items()}
            if isinstance(v, (list, tuple)):
                return [_clean(x) for x in v]
            return v

        return _clean(asdict(self))

    @classmethod
    def from_dict(cls, d: dict) -> "StrategyParams":
        """
        Build from a (possibly partial) dict — unknown keys ignored, missing keys
        take defaults. Coerces the nested lead_time/model_run sequences back to
        the tuple-of-tuples shape the engine expects.
        """
        valid = {f.name for f in __import__("dataclasses").fields(cls)}
        clean = {k: v for k, v in (d or {}).items() if k in valid}
        if "lead_time_factors" in clean and clean["lead_time_factors"] is not None:
            clean["lead_time_factors"] = tuple(tuple(x) for x in clean["lead_time_factors"])
        if "model_run_hours_et" in clean and clean["model_run_hours_et"] is not None:
            clean["model_run_hours_et"] = tuple(tuple(x) for x in clean["model_run_hours_et"])
        return cls(**clean)
