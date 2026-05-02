"""Configuration for Kalshi weather arb bot."""
import os
from pydantic_settings import BaseSettings
from typing import Optional


class Settings(BaseSettings):
    # Database
    DATABASE_URL: str = "sqlite:///./weatherbot.db"

    # Kalshi API
    KALSHI_API_KEY_ID: Optional[str] = None
    KALSHI_PRIVATE_KEY_PATH: Optional[str] = None
    KALSHI_PRIVATE_KEY_PEM: Optional[str] = None  # Inline PEM string (alternative to file)

    # Discord
    DISCORD_WEBHOOK_URL: Optional[str] = None
    DISCORD_BOT_TOKEN: Optional[str] = None    # Bot token for reading messages (optional)
    DISCORD_CHANNEL_ID: Optional[str] = None   # Channel ID to poll for commands

    # Bot settings
    SIMULATION_MODE: bool = True
    INITIAL_BANKROLL: float = 1000.0
    KELLY_FRACTION: float = 0.15
    SCAN_INTERVAL_SECONDS: int = 30
    MIN_EDGE_THRESHOLD: float = 0.08
    KALSHI_FEE_RATE: float = 0.07  # 7% of profit
    WEATHER_MIN_ENTRY_PRICE: float = 0.10  # Skip markets where our entry is < 10¢ (model unreliable at extremes)
    WEATHER_MAX_ENTRY_PRICE: float = 0.70
    WEATHER_MAX_TRADE_SIZE: float = 100.0
    WEATHER_CITIES: str = "nyc,chicago,denver,austin,houston,boston,washington_dc,phoenix,seattle,san_francisco,atlanta,dallas,las_vegas,minneapolis,new_orleans,oklahoma_city,san_antonio,philadelphia"  # miami/los_angeles excluded: model miscalibrated

    # Trading hours gate (US/Eastern) — paper trades and Discord alerts only
    # fire inside this window. Scanning still runs outside hours for data.
    TRADING_HOURS_START: int = 10   # 10:00 AM ET (inclusive)
    TRADING_HOURS_END: int = 18     # 6:00 PM ET (exclusive)

    # Dry run — default TRUE so live trading requires explicit opt-in
    DRY_RUN: bool = True

    # Health check
    PORT: int = 8080

    class Config:
        env_file = ".env"


settings = Settings()
