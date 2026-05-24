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
    INITIAL_BANKROLL: float = 1000.0
    KELLY_FRACTION: float = 0.15
    SCAN_INTERVAL_SECONDS: int = 30
    MIN_EDGE_THRESHOLD: float = 0.08
    KALSHI_FEE_RATE: float = 0.07  # 7% of profit
    WEATHER_MIN_ENTRY_PRICE: float = 0.10
    WEATHER_MAX_ENTRY_PRICE: float = 0.70
    WEATHER_MAX_TRADE_SIZE: float = 100.0
    CITY_OVERRIDE: str = ""  # e.g. "nyc" in .env to scan only one city locally

    # Liquidity filters
    MIN_ASK_SIZE: int = 25
    MIN_VOLUME_24H: int = 200

    # Trading hours gate (US/Eastern) — paper trades and Discord alerts only
    # fire inside this window. Scanning still runs outside hours for data.
    TRADING_HOURS_START: int = 10   # 10:00 AM ET (inclusive)
    TRADING_HOURS_END: int = 18     # 6:00 PM ET (exclusive)

    # Live trading — default FALSE so paper trading requires explicit opt-in to go live
    LIVE_TRADING: bool = False
    # Default to demo URL; prod requires explicit override to prod URL in .env
    KALSHI_API_BASE_URL: str = "https://demo-api.kalshi.co/trade-api/v2"
    # Hard cap per live order in dollars (set low during initial live testing)
    LIVE_MAX_TRADE_SIZE: float = 5.0

    # Health check
    PORT: int = 8080

    class Config:
        env_file = ".env"


settings = Settings()
