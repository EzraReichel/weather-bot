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
                salt_length=padding.PSS.MAX_LENGTH,
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
        self, ticker: str, side: str, count: int, yes_price: int
    ) -> dict:
        """
        Place a limit order. side: 'yes' or 'no'. yes_price: cents (e.g. 65 = $0.65).
        Returns the full API response dict (contains 'order' key with order details).
        """
        return await self._post("/portfolio/orders", {
            "ticker": ticker,
            "action": "buy",
            "side": side,
            "count": count,
            "type": "limit",
            "yes_price": yes_price,
        })

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


async def fetch_live_balance() -> float:
    """
    Return the live Kalshi account balance in dollars.
    Falls back to settings.INITIAL_BANKROLL if credentials are missing or the API fails.
    """
    if not kalshi_credentials_present():
        return settings.INITIAL_BANKROLL
    try:
        data = await KalshiClient().get_balance()
        if "balance_dollars" in data:
            return float(data["balance_dollars"])
        return data.get("balance", 0) / 100.0
    except Exception as e:
        logger.warning(f"fetch_live_balance failed, using INITIAL_BANKROLL: {e}")
        return settings.INITIAL_BANKROLL
