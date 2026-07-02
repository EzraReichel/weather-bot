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

    # Dashboard auth — Bearer token (or ?token= / cookie) required on every route
    # except /health. When unset the dashboard is open ONLY if LIVE_TRADING=false
    # (local dev); under live trading an unset token fails the panel closed (503).
    DASHBOARD_TOKEN: Optional[str] = None

    # Bot settings
    INITIAL_BANKROLL: float = 1000.0
    KELLY_FRACTION: float = 0.15
    # Bankroll basis for Kelly sizing. False = available cash only (default);
    # True = total account equity (cash + current value of open positions).
    KELLY_USE_EQUITY: bool = False
    SCAN_INTERVAL_SECONDS: int = 300
    MIN_EDGE_THRESHOLD: float = 0.08
    KALSHI_FEE_RATE: float = 0.07  # 7% of profit
    WEATHER_MIN_ENTRY_PRICE: float = 0.10
    WEATHER_MAX_ENTRY_PRICE: float = 0.70
    WEATHER_MAX_TRADE_SIZE: float = 100.0
    CITY_OVERRIDE: str = ""  # e.g. "nyc" in .env to scan only one city locally

    # ── Regime gates (decision A — summer bleed control) ──────────────────────
    # Audited on the live ledger (144 resolved, queried 2026-06-16): these two
    # gates turn −$366 as-traded into +$103, and June −$381 → −$13. They cut the
    # two buckets with no demonstrated edge outside the spring cold-snap regime.
    #   • no/above: −$352 over 75 trades (23% win) — the dominant summer loser.
    #   • sub-20¢ entries: 7% win, −$454 — cheap longshots that only paid in the
    #     spring paper run. Layered ON TOP of existing floors, so it only tightens
    #     the NO floor (0.10→0.20) and the cold-day YES floor; the 0.30 YES floor
    #     is unaffected. Flip these off / lower the floor when the cold-snap
    #     regime returns or once the model is recalibrated.
    BLOCK_NO_ABOVE_SIGNALS: bool = True
    REGIME_MIN_ENTRY_PRICE: float = 0.20

    # Liquidity filters
    MIN_ASK_SIZE: int = 25
    MIN_VOLUME_24H: int = 200

    # Live trading — default FALSE so paper trading requires explicit opt-in to go live
    LIVE_TRADING: bool = False
    KALSHI_API_BASE_URL: str = "https://api.elections.kalshi.com/trade-api/v2"
    # Hard cap per live order in dollars (set low during initial live testing)
    LIVE_MAX_TRADE_SIZE: float = 5.0
    # When topping up a partially-filled position on a later scan and our side's
    # ask has fallen more than this many dollars (0-1 scale, e.g. 0.02 = 2¢)
    # below the original fill, only add if our model has actively strengthened.
    # Guards against averaging down into a market that increasingly disagrees
    # with a possibly-stale model (adverse selection).
    TOPUP_MAX_ADVERSE_DROP: float = 0.02
    # Circuit breaker: max number of physical orders allowed per position
    # (original order + top-ups). Caps the order ladder so a thin book that
    # only fills a few contracts per scan can't spawn dozens of tiny adds.
    TOPUP_MAX_ORDERS: int = 4

    # Trading hours
    SAME_DAY_HIGH_CUTOFF_HOUR: int = 9        # stop entering same-day high markets at/after this hour ET
    # LOW cutoff is in each city's LOCAL time (not ET) — the daily low prints near
    # local dawn, so an ET cutoff blocked western cities pre-event and threw away
    # the profitable morning window. Loosened 7→10 local: still blocks clearly
    # post-event afternoon entries while keeping the low metric's best trades.
    SAME_DAY_LOW_CUTOFF_HOUR: int = 10        # stop entering same-day low markets at/after this hour LOCAL
    TRADING_HOURS_CONVICTION_THRESHOLD: float = 0.75  # bypass all time gates when both model AND market >= this

    # Backtest data capture — snapshot every scan into the bt_* archive tables.
    # Best-effort and isolated from trading; set false to disable all capture.
    BACKTEST_CAPTURE: bool = True

    # Health check
    PORT: int = 8080

    class Config:
        env_file = ".env"


settings = Settings()
