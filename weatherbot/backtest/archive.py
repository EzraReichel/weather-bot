"""
Read-only loaders over the bt_* archive.

Loads the whole capture window into memory (it's small — deduped price paths and
content-deduped forecasts) and exposes point-in-time lookups so the portfolio
simulator can rebuild a WeatherDay for any (ticker, as_of) during replay.
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import Dict, List, Optional, Tuple

from sqlalchemy import text

from weatherbot.data.multi_source_weather import SourceForecast
from weatherbot.data.weather import CITY_CONFIG, get_climatology_normal
from weatherbot.data.weather_markets import WeatherMarket
from weatherbot.backtest.world import WeatherDay

logger = logging.getLogger("weatherbot")


def _utc_naive(dt: datetime) -> datetime:
    """Archive captured_at is naive UTC; normalize any as_of to the same basis."""
    if dt.tzinfo is not None:
        return dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt


@dataclass
class TickerMeta:
    ticker: str
    city_key: str
    city_name: str
    metric: str
    direction: str
    threshold_f: float
    target_date: date


@dataclass
class _MarketPoint:
    captured_at: datetime
    yes_ask: float
    yes_bid: float
    no_ask: float


@dataclass
class _WeatherPoint:
    captured_at: datetime
    sources: Dict[str, SourceForecast]
    observation: Optional[dict]


@dataclass
class BacktestData:
    """Everything a replay needs, loaded once from the archive."""
    tickers: Dict[str, TickerMeta] = field(default_factory=dict)
    market_paths: Dict[str, List[_MarketPoint]] = field(default_factory=dict)      # ticker -> points (asc)
    weather: Dict[Tuple[str, str], List[_WeatherPoint]] = field(default_factory=dict)  # (city,date) -> points (asc)
    settlements: Dict[str, dict] = field(default_factory=dict)                     # ticker -> {result, expiration_value}

    # ── point-in-time lookups ────────────────────────────────────────────────
    def market_at(self, ticker: str, as_of: datetime) -> Optional[_MarketPoint]:
        pts = self.market_paths.get(ticker)
        if not pts:
            return None
        naive = _utc_naive(as_of)
        chosen = None
        for p in pts:
            if p.captured_at <= naive:
                chosen = p
        return chosen  # None until the first capture (don't trade before data exists)

    def weather_at(self, city_key: str, target_date: str, as_of: datetime) -> Optional[_WeatherPoint]:
        pts = self.weather.get((city_key, target_date))
        if not pts:
            return None
        naive = _utc_naive(as_of)
        chosen = None
        for p in pts:
            if p.captured_at <= naive:
                chosen = p
        # Forward-fill the earliest content: a forecast written at T was the
        # active content slightly before T too (capture writes a few seconds
        # after the scan that observed it).
        return chosen or pts[0]

    def build_day(self, ticker: str, as_of: datetime) -> Optional[WeatherDay]:
        meta = self.tickers.get(ticker)
        if not meta:
            return None
        mp = self.market_at(ticker, as_of)
        if mp is None:
            return None
        tdate_str = meta.target_date.isoformat()
        wp = self.weather_at(meta.city_key, tdate_str, as_of)

        ccfg = CITY_CONFIG.get(meta.city_key, {})
        grid_bias = ccfg.get("grid_bias", {}).get(meta.metric, 0.0)
        market = WeatherMarket(
            slug=ticker, market_id=ticker, platform="kalshi", title=ticker,
            city_key=meta.city_key, city_name=meta.city_name,
            target_date=meta.target_date, threshold_f=meta.threshold_f,
            metric=meta.metric, direction=meta.direction,
            yes_price=mp.yes_ask or 0.0, no_price=mp.no_ask or 0.0,
            yes_ask=mp.yes_ask or 0.0, yes_bid=mp.yes_bid or 0.0,
        )
        return WeatherDay(
            market=market, as_of=as_of,
            sources=wp.sources if wp else {},
            observation=wp.observation if wp else None,
            climatology_normal=get_climatology_normal(meta.city_key, meta.target_date, meta.metric),
            grid_bias=grid_bias,
        )


def _to_sources(sources_json) -> Dict[str, SourceForecast]:
    src = sources_json if isinstance(sources_json, dict) else json.loads(sources_json or "{}")
    out = {}
    for name, sd in src.items():
        out[name] = SourceForecast(
            source=name,
            member_highs=sd.get("member_highs") or [],
            member_lows=sd.get("member_lows") or [],
            ok=bool(sd.get("ok")),
        )
    return out


def load_archive(db, start: Optional[str] = None, end: Optional[str] = None,
                 cities: Optional[List[str]] = None) -> BacktestData:
    """
    Load the archive for target_dates in [start, end] (inclusive, YYYY-MM-DD) and
    the given cities (None/empty = all). Bounds filter on a market's target_date.
    """
    data = BacktestData()
    where = ["tradeable = TRUE"]
    p: dict = {}
    if start:
        where.append("target_date >= :start"); p["start"] = start
    if end:
        where.append("target_date <= :end"); p["end"] = end
    if cities:
        where.append("city_key = ANY(:cities)"); p["cities"] = list(cities)
    wsql = " AND ".join(where)

    # ── Ticker metadata + market price paths ──────────────────────────────────
    rows = db.execute(text(
        f"SELECT ticker, city_key, city_name, metric, direction, threshold_f, target_date, "
        f"captured_at, yes_ask, yes_bid, no_ask FROM bt_market_snapshots "
        f"WHERE {wsql} ORDER BY captured_at ASC"
    ), p).mappings().all()

    for r in rows:
        tk = r["ticker"]
        if tk not in data.tickers and r["target_date"] and r["metric"]:
            data.tickers[tk] = TickerMeta(
                ticker=tk, city_key=r["city_key"] or "",
                city_name=r["city_name"] or (r["city_key"] or ""),
                metric=r["metric"], direction=r["direction"] or "above",
                threshold_f=r["threshold_f"] or 0.0,
                target_date=date.fromisoformat(r["target_date"]),
            )
        data.market_paths.setdefault(tk, []).append(_MarketPoint(
            captured_at=r["captured_at"],
            yes_ask=r["yes_ask"] or 0.0, yes_bid=r["yes_bid"] or 0.0, no_ask=r["no_ask"] or 0.0,
        ))

    # ── Weather snapshots for the involved (city, date) pairs ─────────────────
    city_dates = {(m.city_key, m.target_date.isoformat()) for m in data.tickers.values()}
    if city_dates:
        wrows = db.execute(text(
            "SELECT city_key, target_date, captured_at, sources, observation "
            "FROM bt_weather_snapshots ORDER BY captured_at ASC"
        )).mappings().all()
        for w in wrows:
            key = (w["city_key"], w["target_date"])
            if key not in city_dates:
                continue
            data.weather.setdefault(key, []).append(_WeatherPoint(
                captured_at=w["captured_at"],
                sources=_to_sources(w["sources"]),
                observation=(w["observation"] if isinstance(w["observation"], dict)
                             else json.loads(w["observation"]) if w["observation"] else None),
            ))

    # ── Ground-truth settlements ──────────────────────────────────────────────
    srows = db.execute(text(
        "SELECT ticker, result, expiration_value FROM bt_settlements"
    )).mappings().all()
    for s in srows:
        if s["result"] in ("yes", "no"):
            data.settlements[s["ticker"]] = {
                "result": s["result"], "expiration_value": s["expiration_value"],
            }

    logger.info(
        f"[bt] archive loaded: {len(data.tickers)} tickers, "
        f"{sum(len(v) for v in data.market_paths.values())} market points, "
        f"{len(data.weather)} city/date forecasts, {len(data.settlements)} settlements"
    )
    return data
