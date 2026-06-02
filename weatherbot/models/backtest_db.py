"""
Backtest data-collection schema.

These ``bt_*`` tables are a *forward archive* of everything the live scanner
sees, captured point-in-time so trading strategies can be replayed honestly
later. They live on the same Render Postgres as the trading tables but are
completely independent of them — nothing here touches order placement.

Why capture live instead of backfilling?
    The live probability model weights ``ensemble_fraction`` at ~70%, which
    needs *individual ensemble member* temperatures. Open-Meteo's Previous-Runs
    API only serves the ensemble *mean* historically (see
    scripts/verify_backtest_data.py), so the member arrays cannot be
    reconstructed after the fact. We must snapshot them as the scan runs.

Tables
    bt_scans            — one row per scan run (the capture envelope)
    bt_market_snapshots — every market seen in a scan (price + liquidity)
    bt_weather_snapshots— per-source ensemble member arrays, deduped by content
    bt_signal_snapshots — the model's output for each market at scan time
    bt_settlements      — ground-truth outcome per ticker (backfilled)

A backtest joins: market_snapshot (entry price at time T) → weather_snapshot
(point-in-time forecast for that city/date) → settlement (actual result), then
applies any candidate entry rule and tallies P&L.
"""
from datetime import datetime

from sqlalchemy import (
    Column, Integer, String, Float, DateTime, Boolean, JSON, Index, UniqueConstraint,
)
from sqlalchemy.ext.declarative import declarative_base

from weatherbot.models.weather_db import engine

BacktestBase = declarative_base()


class BtScan(BacktestBase):
    """One scan run — the envelope that ties snapshots together."""
    __tablename__ = "bt_scans"

    id                  = Column(Integer, primary_key=True)
    scan_id             = Column(String, unique=True, index=True)   # uuid hex
    started_at          = Column(DateTime, default=datetime.utcnow, index=True)
    completed_at        = Column(DateTime, nullable=True)

    trigger             = Column(String, default="interval")   # interval | model_run | startup | manual
    live_trading        = Column(Boolean, default=False)
    bankroll            = Column(Float, nullable=True)

    markets_tradeable   = Column(Integer, default=0)
    markets_filtered    = Column(Integer, default=0)
    signals_total       = Column(Integer, default=0)
    signals_actionable  = Column(Integer, default=0)
    weather_captured    = Column(Integer, default=0)   # new weather rows written this scan


class BtMarketSnapshot(BacktestBase):
    """
    A market as seen at scan time. Deduped per-ticker: a new row is only
    written when yes_ask / yes_bid / no_price / volume_24h changes, so the
    table records the *price path* without storing 288 identical rows/day.
    """
    __tablename__ = "bt_market_snapshots"

    id                 = Column(Integer, primary_key=True)
    scan_id            = Column(String, index=True)
    captured_at        = Column(DateTime, default=datetime.utcnow, index=True)

    ticker             = Column(String, index=True)
    series_ticker      = Column(String, nullable=True)
    city_key           = Column(String, index=True)
    city_name          = Column(String, nullable=True)
    metric             = Column(String, index=True)        # high | low | rain
    direction          = Column(String)                    # above | below
    threshold_f        = Column(Float)
    target_date        = Column(String, index=True)        # YYYY-MM-DD
    days_to_resolution = Column(Integer, nullable=True)

    yes_ask            = Column(Float, nullable=True)
    yes_bid            = Column(Float, nullable=True)
    no_ask             = Column(Float, nullable=True)
    last_price         = Column(Float, nullable=True)
    volume             = Column(Float, nullable=True)
    volume_24h         = Column(Float, nullable=True)
    yes_ask_size       = Column(Float, nullable=True)
    open_interest      = Column(Float, nullable=True)

    tradeable          = Column(Boolean, default=True, index=True)
    filter_reason      = Column(String, nullable=True)     # set when tradeable=False

    __table_args__ = (
        Index("ix_bt_market_ticker_time", "ticker", "captured_at"),
        Index("ix_bt_market_city_date", "city_key", "target_date"),
    )


class BtWeatherSnapshot(BacktestBase):
    """
    Per-source ensemble forecast for one (city, target_date) at a point in time.

    Deduped by ``content_hash`` (city + date + the member arrays), so each row
    is a *distinct* forecast — i.e. the table is the forecast's evolution as
    new model runs land, not one row per scan.

    ``sources`` JSON shape::

        {
          "gfs":   {"member_highs": [...], "member_lows": [...],
                    "mean_high": 71.2, "std_high": 1.8,
                    "mean_low": 55.1, "std_low": 1.3, "n": 31, "ok": true},
          "ecmwf": {...}, "gem": {...}, "nws": {...}
        }
    """
    __tablename__ = "bt_weather_snapshots"

    id           = Column(Integer, primary_key=True)
    scan_id      = Column(String, index=True)
    captured_at  = Column(DateTime, default=datetime.utcnow, index=True)

    city_key     = Column(String, index=True)
    target_date  = Column(String, index=True)       # YYYY-MM-DD
    content_hash = Column(String, unique=True, index=True)

    sources      = Column(JSON)                      # see docstring
    observation  = Column(JSON, nullable=True)       # {current_temp_f, observed_max_f, observed_min_f, obs_time}

    __table_args__ = (
        Index("ix_bt_weather_city_date_time", "city_key", "target_date", "captured_at"),
    )


class BtSignalSnapshot(BacktestBase):
    """
    The model's output for a market at scan time — every signal, including
    filtered ones (edge zeroed). Lets a backtest replay threshold/sizing
    changes without re-running the forecast model.
    """
    __tablename__ = "bt_signal_snapshots"

    id                     = Column(Integer, primary_key=True)
    scan_id                = Column(String, index=True)
    captured_at            = Column(DateTime, default=datetime.utcnow, index=True)

    ticker                 = Column(String, index=True)
    city_key               = Column(String, index=True)
    metric                 = Column(String)
    direction              = Column(String)        # market direction: above | below
    threshold_f            = Column(Float)
    target_date            = Column(String, index=True)

    model_probability      = Column(Float)
    market_probability     = Column(Float)
    edge                   = Column(Float)
    bet_side               = Column(String)        # yes | no — the side the signal favors
    confidence             = Column(Float)
    kelly_fraction         = Column(Float)
    suggested_size         = Column(Float)
    entry_price            = Column(Float)         # price we'd pay on bet_side

    ensemble_mean          = Column(Float)
    ensemble_std           = Column(Float)
    ensemble_members       = Column(Integer)
    agreement              = Column(String)
    sources_used           = Column(JSON)
    source_probs           = Column(JSON)
    low_confidence_flag    = Column(Boolean, default=False)

    filter_reason          = Column(String, nullable=True)
    passes_threshold       = Column(Boolean, default=False)
    passes_paper_threshold = Column(Boolean, default=False)
    reasoning              = Column(String, nullable=True)

    __table_args__ = (
        Index("ix_bt_signal_ticker_time", "ticker", "captured_at"),
    )


class BtSettlement(BacktestBase):
    """Ground-truth outcome for a ticker, backfilled from Kalshi historical API."""
    __tablename__ = "bt_settlements"

    id               = Column(Integer, primary_key=True)
    ticker           = Column(String, unique=True, index=True)
    city_key         = Column(String, index=True)
    metric           = Column(String)
    direction        = Column(String)
    threshold_f      = Column(Float)
    target_date      = Column(String, index=True)

    result           = Column(String)            # yes | no
    expiration_value = Column(Float, nullable=True)   # actual observed temp (settlement value)
    settled_ts       = Column(DateTime, nullable=True)
    filled_at        = Column(DateTime, default=datetime.utcnow)


def init_backtest_db():
    """Create the bt_* tables if they don't exist (idempotent)."""
    BacktestBase.metadata.create_all(bind=engine)
