"""Trade model — shared by paper and live trades. Use is_paper to distinguish."""
from datetime import datetime

from sqlalchemy import Column, Integer, String, Float, DateTime, Boolean, text
from sqlalchemy.ext.declarative import declarative_base

from weatherbot.models.weather_db import engine, SessionLocal

TradeBase = declarative_base()


class Trade(TradeBase):
    """One logged trade — paper or live — from a signal that passed threshold."""
    __tablename__ = "trades"

    id               = Column(Integer, primary_key=True, index=True)
    is_paper         = Column(Boolean, default=True, nullable=False)

    ticker           = Column(String, index=True)
    city             = Column(String)
    metric           = Column(String)                       # "high", "low", or "rain"
    threshold_f      = Column(Float)
    side             = Column(String)                       # "yes" or "no"
    market_direction = Column(String)                       # "above" or "below"
    agreement        = Column(String, default="MEDIUM")     # "HIGH", "MEDIUM", "LOW"
    model_probs      = Column(String, nullable=True)        # JSON: {"gfs": 0.72, ...}

    # Signal details at entry
    model_prob       = Column(Float)
    market_price     = Column(Float)
    edge             = Column(Float)
    confidence       = Column(Float)
    kelly_size       = Column(Float)
    contracts        = Column(Integer)
    entry_price      = Column(Float)

    # Forecast details
    forecast_mean    = Column(Float)
    forecast_std     = Column(Float)

    # Timestamps
    created_at       = Column(DateTime, default=datetime.utcnow)
    resolution_date  = Column(String)                       # YYYY-MM-DD

    # Live trading fields (null for paper trades)
    kalshi_order_id  = Column(String, nullable=True)
    fill_price       = Column(Float, nullable=True)

    # Settlement
    actual_temp      = Column(Float, nullable=True)         # 1.0=YES won, 0.0=NO won
    resolved         = Column(Boolean, default=False)
    result           = Column(String, nullable=True)        # "win", "loss", or "push"
    pnl              = Column(Float, nullable=True)
    resolved_at      = Column(DateTime, nullable=True)


class ModelCityAccuracy(TradeBase):
    """Tracks per-model, per-city Brier score and win rate as trades settle."""
    __tablename__ = "model_city_accuracy"

    id         = Column(Integer, primary_key=True)
    model      = Column(String, index=True)
    city       = Column(String, index=True)
    metric     = Column(String)
    n          = Column(Integer, default=0)
    brier_sum  = Column(Float, default=0.0)
    wins       = Column(Integer, default=0)
    losses     = Column(Integer, default=0)
    updated_at = Column(DateTime, default=datetime.utcnow)


def init_trade_db():
    _migrate()
    TradeBase.metadata.create_all(bind=engine)


def _migrate():
    is_pg = engine.dialect.name == "postgresql"
    with engine.connect() as conn:
        from sqlalchemy import inspect
        inspector = inspect(engine)
        existing_tables = inspector.get_table_names()

        # Rename paper_trades → trades if the old table exists and new one doesn't
        if "paper_trades" in existing_tables and "trades" not in existing_tables:
            with conn.begin():
                conn.execute(text("ALTER TABLE paper_trades RENAME TO trades"))
            inspector = inspect(engine)  # refresh after rename

        # Get current columns (if table exists)
        try:
            cols = [c["name"] for c in inspector.get_columns("trades")]
        except Exception:
            return  # table doesn't exist yet — create_all will handle it

        # If the trades table is the old BTC-era schema (missing 'ticker'),
        # drop and recreate — that data has no value for the weather bot.
        if "ticker" not in cols:
            with conn.begin():
                conn.execute(text("DROP TABLE trades"))
            return  # create_all will build the fresh schema

        # Add columns introduced after initial weather-bot schema
        new_cols = [
            ("is_paper",        "BOOLEAN DEFAULT 1 NOT NULL"),
            ("agreement",       "VARCHAR DEFAULT 'MEDIUM'"),
            ("model_probs",     "TEXT"),
            ("kalshi_order_id", "VARCHAR"),
            ("fill_price",      "FLOAT"),
        ]
        for col, typedef in new_cols:
            if col not in cols:
                with conn.begin():
                    if is_pg:
                        conn.execute(text(
                            f"ALTER TABLE trades ADD COLUMN IF NOT EXISTS {col} {typedef}"
                        ))
                    else:
                        conn.execute(text(f"ALTER TABLE trades ADD COLUMN {col} {typedef}"))
