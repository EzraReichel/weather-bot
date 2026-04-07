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

    # Bot settings
    SIMULATION_MODE: bool = True
    INITIAL_BANKROLL: float = 1000.0
    KELLY_FRACTION: float = 0.15
    SCAN_INTERVAL_SECONDS: int = 30
    MIN_EDGE_THRESHOLD: float = 0.08
    KALSHI_FEE_RATE: float = 0.07  # 7% of profit
    WEATHER_MAX_ENTRY_PRICE: float = 0.70
    WEATHER_MAX_TRADE_SIZE: float = 100.0
    WEATHER_CITIES: str = "nyc,chicago,miami,los_angeles,denver"

    # Dry run — default TRUE so live trading requires explicit opt-in
    DRY_RUN: bool = True

    # Health check
    PORT: int = 8080

    class Config:
        env_file = ".env"


settings = Settings()
