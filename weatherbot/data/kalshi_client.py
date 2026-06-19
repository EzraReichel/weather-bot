"""Kalshi API client with RSA-PSS signature authentication."""
import base64
import logging
import time
from pathlib import Path
from typing import Any, Dict, Optional

import httpx
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

from weatherbot.config import settings

logger = logging.getLogger("weatherbot")


class KalshiClient:
    """Async Kalshi API client using RSA-PSS signature auth."""

    def __init__(self):
        self._private_key = None

    def _load_private_key(self):
        """Load RSA private key from file or inline PEM (lazy, cached)."""
        if self._private_key is not None:
            return self._private_key

        # Prefer file path (local dev); fall back to inline PEM (Railway/production)
        if settings.KALSHI_PRIVATE_KEY_PATH:
            key_path = Path(settings.KALSHI_PRIVATE_KEY_PATH).expanduser()
            if not key_path.exists():
                raise FileNotFoundError(
                    f"KALSHI_PRIVATE_KEY_PATH points to missing file: {key_path}"
                )
            pem_data = key_path.read_bytes()
            logger.debug(f"Loaded PEM from file: {key_path}")
        elif settings.KALSHI_PRIVATE_KEY_PEM:
            pem_data = settings.KALSHI_PRIVATE_KEY_PEM.encode("utf-8")
            # Handle \n-escaped newlines (common when pasting into env vars)
            if b"\\n" in pem_data:
                pem_data = pem_data.replace(b"\\n", b"\n")
            logger.debug("Loaded PEM from inline KALSHI_PRIVATE_KEY_PEM")
        else:
            raise ValueError(
                "No Kalshi private key configured.\n"
                "  Local:  set KALSHI_PRIVATE_KEY_PATH=./kalshi_key.pem\n"
                "  Deploy: set KALSHI_PRIVATE_KEY_PEM=<inline PEM string>"
            )

        # Sanity-check: should start with -----BEGIN
        if not pem_data.lstrip().startswith(b"-----BEGIN"):
            preview = pem_data[:40].decode("utf-8", errors="replace")
            raise ValueError(
                f"PEM data doesn't look right (first 40 chars): {preview!r}\n"
                "Make sure it starts with '-----BEGIN RSA PRIVATE KEY-----' or similar."
            )

        self._private_key = serialization.load_pem_private_key(pem_data, password=None)
        return self._private_key

    @property
    def _base_url(self) -> str:
        return settings.KALSHI_API_BASE_URL

    @property
    def _path_prefix(self) -> str:
        # Extract the path portion after the host for signing (e.g. /trade-api/v2)
        from urllib.parse import urlparse
        return urlparse(self._base_url).path

    def _sign_request(self, method: str, path: str) -> Dict[str, str]:
        """
        Generate auth headers for a Kalshi API request.
        Signature = RSA-PSS-sign(timestamp_ms + METHOD + path)
        """
        timestamp_ms = str(int(time.time() * 1000))
        message = f"{timestamp_ms}{method.upper()}{path}"

        private_key = self._load_private_key()
        signature = private_key.sign(
            message.encode("utf-8"),
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.DIGEST_LENGTH,
            ),
            hashes.SHA256(),
        )

        return {
            "KALSHI-ACCESS-KEY": settings.KALSHI_API_KEY_ID,
            "KALSHI-ACCESS-SIGNATURE": base64.b64encode(signature).decode("utf-8"),
            "KALSHI-ACCESS-TIMESTAMP": timestamp_ms,
            "Content-Type": "application/json",
        }

    async def get(self, path: str, params: Optional[Dict[str, Any]] = None) -> dict:
        """Authenticated GET request to Kalshi API."""
        full_path = f"{self._path_prefix}{path}"
        url = f"{self._base_url}{path}"
        headers = self._sign_request("GET", full_path)

        async with httpx.AsyncClient(timeout=15.0) as client:
            response = await client.get(url, headers=headers, params=params)
            response.raise_for_status()
            return response.json()

    async def _post(self, path: str, body: Dict[str, Any]) -> dict:
        """Authenticated POST request to Kalshi API."""
        full_path = f"{self._path_prefix}{path}"
        url = f"{self._base_url}{path}"
        headers = self._sign_request("POST", full_path)

        async with httpx.AsyncClient(timeout=15.0) as client:
            response = await client.post(url, headers=headers, json=body)
            response.raise_for_status()
            return response.json()

    async def _delete(self, path: str) -> dict:
        """Authenticated DELETE request to Kalshi API."""
        full_path = f"{self._path_prefix}{path}"
        url = f"{self._base_url}{path}"
        headers = self._sign_request("DELETE", full_path)

        async with httpx.AsyncClient(timeout=15.0) as client:
            response = await client.delete(url, headers=headers)
            response.raise_for_status()
            return response.json()

    async def get_markets(self, params: Optional[Dict[str, Any]] = None) -> dict:
        """Fetch markets with optional filters."""
        return await self.get("/markets", params=params)

    async def get_market(self, ticker: str) -> dict:
        """Fetch a single market by ticker."""
        return await self.get(f"/markets/{ticker}")

    async def get_balance(self) -> dict:
        """Get portfolio balance (useful for auth test)."""
        return await self.get("/portfolio/balance")

    async def place_order(
        self, ticker: str, side: str, count: int,
        yes_price: Optional[int] = None, no_price: Optional[int] = None,
    ) -> dict:
        """
        Place a resting limit order via Kalshi's V2 endpoint
        (POST /portfolio/events/orders). side: 'yes' or 'no'; prices in cents on
        the side being bought (e.g. 65 = $0.65).

        The signature is unchanged from the legacy client so trading.py needs no
        edits — we translate to the V2 schema here. The differences that matter:

          • V2 has NO yes/no field; everything is quoted from the YES side:
            buy YES = side 'bid', buy NO = side 'ask' (economically sell YES).
          • `price` is ALWAYS the YES price, in DOLLARS. A NO buy at `no_price`
            cents therefore sends a YES price of (100 - no_price)/100.
          • count/price are fixed-point strings; time_in_force and
            self_trade_prevention_type are required.

        The legacy POST /portfolio/orders (side yes/no, cents) was retired by
        Kalshi and now returns HTTP 410 — do not resurrect it.

        Returns a dict normalized back to the legacy response shape (order id +,
        on an immediate fill, a per-side fill price in cents) so existing callers
        keep working.
        """
        # Resolve a single YES-perspective limit price in whole cents.
        if side == "no":
            if no_price is None:
                if yes_price is None:
                    raise ValueError("NO order requires no_price (or yes_price to convert)")
                no_price = 100 - yes_price
            yes_limit_cents = 100 - no_price   # YES price = 1 − NO price
            order_side = "ask"                 # buy NO == sell YES
        else:
            if yes_price is None:
                if no_price is None:
                    raise ValueError("YES order requires yes_price (or no_price to convert)")
                yes_price = 100 - no_price
            yes_limit_cents = yes_price
            order_side = "bid"                 # buy YES

        body = {
            "ticker": ticker,
            "side": order_side,
            "count": str(int(count)),
            "price": f"{yes_limit_cents / 100:.4f}",   # YES price in dollars
            "time_in_force": "good_till_canceled",      # rest until filled/cancelled, as before
            "self_trade_prevention_type": "taker_at_cross",
        }
        resp = await self._post("/portfolio/events/orders", body)
        return self._normalize_order_response(resp, side)

    @staticmethod
    def _normalize_order_response(resp: dict, side: str) -> dict:
        """Map a V2 create-order response back to the legacy shape trading.py reads.

        Legacy callers expect an order id under `id`/`order_id` and, on an
        immediate fill, a per-side fill price in CENTS (`yes_price` for a YES buy,
        `no_price` for a NO buy). V2 returns a flat body with `order_id` and —
        only when it filled on placement — `average_fill_price` (a YES price in
        dollars). When unfilled we omit the price so the caller falls back to the
        limit (entry) price; settlement later reconciles from actual fills.
        """
        order_id = resp.get("order_id") or resp.get("id")
        out = {"id": order_id, "order_id": order_id, "raw": resp}

        avg = resp.get("average_fill_price")
        if avg is not None:
            yes_fill_cents = round(float(avg) * 100)
            if side == "no":
                out["no_price"] = 100 - yes_fill_cents   # back to NO cents
            else:
                out["yes_price"] = yes_fill_cents
        return out

    async def cancel_order(self, order_id: str) -> dict:
        """Cancel an open order by ID."""
        return await self._delete(f"/portfolio/orders/{order_id}")

    async def get_order(self, order_id: str) -> dict:
        """Fetch a single order by ID — used to check fill status."""
        return await self.get(f"/portfolio/orders/{order_id}")


def kalshi_credentials_present() -> bool:
    """Check if Kalshi API credentials are configured."""
    has_key = bool(settings.KALSHI_API_KEY_ID)
    has_pem = bool(settings.KALSHI_PRIVATE_KEY_PEM or settings.KALSHI_PRIVATE_KEY_PATH)
    return has_key and has_pem


def account_cash_dollars(balance_data: dict) -> float:
    """Available cash from a /portfolio/balance response, in dollars.

    This is the only money that can actually fund a *new* order — use it for
    affordability preflight checks, NOT for Kelly sizing.
    """
    if "balance_dollars" in balance_data:
        return float(balance_data["balance_dollars"])
    return balance_data.get("balance", 0) / 100.0


def account_equity_dollars(balance_data: dict) -> float:
    """Total account value from a /portfolio/balance response, in dollars.

    Equity = available cash + current market value of all open positions.
    Kalshi's /portfolio/balance returns `balance`/`balance_dollars` (cash) and
    `portfolio_value` (cents — current value of all positions held). Use this as
    the Kelly bankroll so sizing tracks full equity, not just idle cash.

    If `portfolio_value` is absent the result degrades to cash-only.
    """
    cash = account_cash_dollars(balance_data)
    positions = balance_data.get("portfolio_value", 0) / 100.0
    return cash + positions


async def fetch_balance_breakdown() -> dict:
    """Return the live account composition in dollars: {cash, positions, equity}.

    `equity` = cash + current market value of open positions. Falls back to
    INITIAL_BANKROLL (as cash, zero positions) if credentials are missing or the
    API fails — so callers always get a usable, internally-consistent triple.
    """
    fallback = {
        "cash": settings.INITIAL_BANKROLL,
        "positions": 0.0,
        "equity": settings.INITIAL_BANKROLL,
    }
    if not kalshi_credentials_present():
        return fallback
    try:
        data = await KalshiClient().get_balance()
        cash = account_cash_dollars(data)
        equity = account_equity_dollars(data)
        return {"cash": cash, "positions": equity - cash, "equity": equity}
    except Exception as e:
        logger.warning(f"fetch_balance_breakdown failed, using INITIAL_BANKROLL: {e}")
        return fallback


async def fetch_live_balance() -> float:
    """
    Return the live Kalshi bankroll in dollars, matching the KELLY_USE_EQUITY
    basis: total equity (cash + open position value) when enabled, else cash.
    Falls back to settings.INITIAL_BANKROLL if credentials are missing or the API fails.
    """
    if not kalshi_credentials_present():
        return settings.INITIAL_BANKROLL
    try:
        data = await KalshiClient().get_balance()
        if settings.KELLY_USE_EQUITY:
            return account_equity_dollars(data)
        return account_cash_dollars(data)
    except Exception as e:
        logger.warning(f"fetch_live_balance failed, using INITIAL_BANKROLL: {e}")
        return settings.INITIAL_BANKROLL
