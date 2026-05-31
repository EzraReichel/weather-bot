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

    # Live trading — default FALSE so paper trading requires explicit opt-in to go live
    LIVE_TRADING: bool = False
    KALSHI_API_BASE_URL: str = "https://api.elections.kalshi.com/trade-api/v2"
    # Hard cap per live order in dollars (set low during initial live testing)
    LIVE_MAX_TRADE_SIZE: float = 5.0

    # Trading hours
    SAME_DAY_HIGH_CUTOFF_HOUR: int = 9        # stop entering same-day high markets at/after this hour ET
    SAME_DAY_LOW_CUTOFF_HOUR: int = 7         # stop entering same-day low markets at/after this hour ET
    TRADING_HOURS_CONVICTION_THRESHOLD: float = 0.75  # bypass all time gates when both model AND market >= this

    # Health check
    PORT: int = 8080

    class Config:
        env_file = ".env"


settings = Settings()
