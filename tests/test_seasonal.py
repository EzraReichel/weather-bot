"""
Unit tests for the season-aware calibration scaffolding.

Covers the pure pieces (no DB): season bucketing, the grid-bias fallback chain,
StrategyParams.for_season identity/override semantics, JSON round-trip with
seasonal overrides, and the seasonal bias bucketing in the calibration harness.
"""
import sys
from datetime import date
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from weatherbot.core.strategy import StrategyParams, season_for
from weatherbot.data import weather as weather_mod
from weatherbot.data.weather import get_grid_bias


def test_season_for_boundaries():
    assert season_for(date(2026, 12, 1)) == "DJF"
    assert season_for(date(2026, 1, 15)) == "DJF"
    assert season_for(date(2026, 2, 28)) == "DJF"
    assert season_for(date(2026, 3, 1)) == "MAM"
    assert season_for(date(2026, 5, 31)) == "MAM"
    assert season_for(date(2026, 6, 1)) == "JJA"
    assert season_for(date(2026, 8, 31)) == "JJA"
    assert season_for(date(2026, 9, 1)) == "SON"
    assert season_for(date(2026, 11, 30)) == "SON"


def test_get_grid_bias_fallback_chain(monkeypatch):
    cfg = {
        "seasonal_city": {
            "grid_bias": {"high": -1.0, "low": -2.0},
            "grid_bias_seasonal": {"JJA": {"high": -3.5, "low": -4.0}},
        },
        "flat_city": {"grid_bias": {"high": 1.0, "low": 2.0}},
        "bare_city": {},
    }
    monkeypatch.setattr(weather_mod, "CITY_CONFIG", cfg)

    jun, dec = date(2026, 6, 15), date(2026, 12, 15)
    # (a) seasonal bucket present for the date's season → seasonal value
    assert get_grid_bias("seasonal_city", "high", jun) == -3.5
    assert get_grid_bias("seasonal_city", "low", jun) == -4.0
    # (a') seasonal block exists but not for this season → flat fallback
    assert get_grid_bias("seasonal_city", "high", dec) == -1.0
    # (b) no seasonal block → flat grid_bias
    assert get_grid_bias("flat_city", "high", jun) == 1.0
    # (c) nothing → 0.0
    assert get_grid_bias("bare_city", "high", jun) == 0.0
    assert get_grid_bias("unknown_city", "low", jun) == 0.0


def test_for_season_identity_when_empty():
    p = StrategyParams.live_default().with_(seasonal_overrides={})
    # identity: same object back, no allocation
    assert p.for_season(date(2026, 6, 15)) is p


def test_for_season_identity_when_season_absent():
    p = StrategyParams().with_(seasonal_overrides={"DJF": {"std_floor_high": 9.0}})
    # June is JJA; only DJF override exists → identity
    assert p.for_season(date(2026, 6, 15)) is p


def test_for_season_applies_only_whitelisted_fields():
    p = StrategyParams().with_(seasonal_overrides={
        "JJA": {
            "std_floor_high": 4.5,
            "ensemble_fraction_weight": 0.6,
            "gaussian_cdf_weight": 0.4,
            "kelly_fraction": 0.99,        # NOT whitelisted — must be ignored
        }
    })
    q = p.for_season(date(2026, 7, 1))
    assert q is not p
    assert q.std_floor_high == 4.5
    assert q.ensemble_fraction_weight == 0.6
    assert q.gaussian_cdf_weight == 0.4
    # everything outside the whitelist is unchanged
    assert q.kelly_fraction == p.kelly_fraction
    assert q.std_floor_low == p.std_floor_low
    assert q.source_weights == p.source_weights
    assert q.min_edge_threshold == p.min_edge_threshold


def test_to_from_dict_round_trip_with_overrides():
    p = StrategyParams().with_(seasonal_overrides={
        "JJA": {"std_floor_high": 4.0, "std_floor_low": 2.5,
                "ensemble_fraction_weight": 0.65, "gaussian_cdf_weight": 0.35},
    })
    q = StrategyParams.from_dict(p.to_dict())
    assert q == p
    assert q.seasonal_overrides == p.seasonal_overrides


def test_estimate_bias_seasonal_buckets_and_min_samples():
    from weatherbot.backtest.calibration import CalEvent, ForecastPoint, estimate_bias, estimate_bias_seasonal
    from datetime import datetime

    weights = {"gfs": 1.0}

    def ev(d: date, observed: float, center: float) -> CalEvent:
        # one source, one member equal to `center` → weighted center == center
        return CalEvent(
            ticker="t", city_key="nyc", metric="high", direction="above",
            threshold_f=80.0, target_date=d, observed_f=observed, outcome=1,
            forecast=ForecastPoint(captured_at=datetime(2026, 6, 1), members={"gfs": [center]}),
            lead_hours=24.0,
        )

    # 6 June days (JJA) with observed-center = +2.0 each; plenty for min_samples
    events = [ev(date(2026, 6, d), observed=82.0, center=80.0) for d in range(1, 7)]
    seasonal = estimate_bias_seasonal(events, weights, min_samples=5)
    assert set(seasonal.keys()) == {("JJA", "nyc", "high")}
    assert abs(seasonal[("JJA", "nyc", "high")] - 2.0) < 1e-9

    # single-season seasonal bias equals the flat bias on the same events
    flat = estimate_bias(events, weights, min_samples=5)
    assert abs(seasonal[("JJA", "nyc", "high")] - flat[("nyc", "high")]) < 1e-9

    # below min_samples → bucket dropped
    assert estimate_bias_seasonal(events[:4], weights, min_samples=5) == {}
