"""Paper trading database — separate from main weatherbot.db."""
from datetime import datetime

from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

PAPER_DB_URL = "sqlite:///./paper_trades.db"

paper_engine = create_engine(
    PAPER_DB_URL,
    connect_args={"check_same_thread": False},
)
PaperSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=paper_engine)
PaperBase = declarative_base()


class PaperTrade(PaperBase):
    """One logged paper trade from a signal that passed threshold."""
    __tablename__ = "paper_trades"

    id              = Column(Integer, primary_key=True, index=True)
    ticker          = Column(String, index=True)          # Kalshi ticker
    city            = Column(String)                       # e.g. "nyc"
    metric          = Column(String)                       # "high" or "low"
    threshold_f     = Column(Float)                        # temperature threshold
    side            = Column(String)                       # "yes" or "no"
    market_direction = Column(String)                      # "above" or "below" (market definition)
    agreement        = Column(String, default="MEDIUM")    # "HIGH", "MEDIUM", "LOW" — for calibration
    model_probs      = Column(String, nullable=True)        # JSON: {"gfs": 0.72, "ecmwf": 0.68, ...}

    # Signal details at entry
    model_prob      = Column(Float)
    market_price    = Column(Float)
    edge            = Column(Float)
    confidence      = Column(Float)
    kelly_size      = Column(Float)                        # dollar amount suggested
    contracts       = Column(Integer)                      # floor(kelly_size / entry_price)
    entry_price     = Column(Float)                        # market ask at time of signal

    # Forecast details
    forecast_mean   = Column(Float)
    forecast_std    = Column(Float)

    # Timestamps
    created_at      = Column(DateTime, default=datetime.utcnow)
    resolution_date = Column(String)                       # YYYY-MM-DD string

    # Settlement (filled in later)
    actual_temp     = Column(Float, nullable=True)
    resolved        = Column(Boolean, default=False)
    result          = Column(String, nullable=True)        # "win", "loss", or "push"
    pnl             = Column(Float, nullable=True)         # net dollar P&L
    resolved_at     = Column(DateTime, nullable=True)


class ModelCityAccuracy(PaperBase):
    """Tracks per-model, per-city Brier score and win rate as trades settle."""
    __tablename__ = "model_city_accuracy"

    id         = Column(Integer, primary_key=True)
    model      = Column(String, index=True)   # "gfs", "ecmwf", "gem", "nws"
    city       = Column(String, index=True)
    metric     = Column(String)               # "high" or "low"
    n          = Column(Integer, default=0)   # trades settled
    brier_sum  = Column(Float, default=0.0)   # sum of (model_prob - actual_outcome)^2
    wins       = Column(Integer, default=0)
    losses     = Column(Integer, default=0)
    updated_at = Column(DateTime, default=datetime.utcnow)


def init_paper_db():
    PaperBase.metadata.create_all(bind=paper_engine)
    _migrate()


def _migrate():
    """Add columns introduced after initial schema without dropping the DB."""
    from sqlalchemy import inspect, text
    inspector = inspect(paper_engine)
    try:
        cols = [c["name"] for c in inspector.get_columns("paper_trades")]
        with paper_engine.connect() as conn:
            for col, typedef in [
                ("agreement",   "VARCHAR DEFAULT 'MEDIUM'"),
                ("model_probs", "TEXT"),
            ]:
                if col not in cols:
                    with conn.begin():
                        conn.execute(text(f"ALTER TABLE paper_trades ADD COLUMN {col} {typedef}"))
    except Exception:
        pass
