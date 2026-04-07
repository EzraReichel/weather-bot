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


def init_paper_db():
    PaperBase.metadata.create_all(bind=paper_engine)
