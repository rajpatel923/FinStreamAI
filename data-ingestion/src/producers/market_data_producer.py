import time
import uuid
from typing import Any

import httpx
import structlog

from src.config.data_source_config import data_source_config
from src.producers.base_producer import BaseProducer
from src.schemas.registry import avro_serializer
from src.utils.data_validation import validator
from src.utils.mock_data import WATCHED_SYMBOLS, mock_generator
from src.utils.monitoring import mock_messages_total, validation_failures_total
from src.utils.rate_limiter import TokenBucket

logger = structlog.get_logger(__name__)


class MarketDataProducer(BaseProducer):
    """Produces real-time market tick data to market.ticks.raw."""

    def __init__(self) -> None:
        super().__init__("market_data_producer", "market.ticks.raw")
        cfg = data_source_config
        self._polygon_key = cfg.POLYGON_API_KEY
        self._av_key = cfg.ALPHA_VANTAGE_API_KEY
        self._finnhub_key = cfg.FINNHUB_API_KEY
        self._use_mock = cfg.USE_MOCK_DATA or (
            not self._polygon_key and not self._av_key and not self._finnhub_key
        )
        self._symbols = cfg.watched_symbols_list
        self._rate_limiter = TokenBucket(rate_per_minute=cfg.POLYGON_RATE_LIMIT)
        self._http = httpx.Client(timeout=cfg.HTTP_TIMEOUT_SECONDS)

        source = "mock" if self._use_mock else self._active_source()
        logger.info("MarketDataProducer initialised", source=source, symbols=self._symbols)

    def _active_source(self) -> str:
        if self._polygon_key:
            return "polygon"
        if self._av_key:
            return "alpha_vantage"
        if self._finnhub_key:
            return "finnhub"
        return "mock"

    def data_source_name(self) -> str:
        if self._use_mock:
            return "mock"
        return self._active_source()

    def poll_interval_seconds(self) -> float:
        if self._use_mock:
            return 30.0
        if self._polygon_key:
            return 1.0
        if self._av_key:
            return 12.0
        return 10.0  # finnhub REST /quote

    def fetch_data(self) -> list[dict]:
        if self._use_mock:
            return [mock_generator.market_tick(sym) for sym in self._symbols]
        if self._polygon_key:
            return self._fetch_polygon()
        if self._av_key:
            return self._fetch_alpha_vantage()
        return self._fetch_finnhub()

    def _fetch_polygon(self) -> list[dict]:
        ticks: list[dict] = []
        base = data_source_config.POLYGON_BASE_URL
        for sym in self._symbols:
            self._rate_limiter.acquire()
            try:
                resp = self._http.get(
                    f"{base}/v2/last/trade/{sym}",
                    params={"apiKey": self._polygon_key},
                )
                resp.raise_for_status()
                result = resp.json().get("results", {})
                ticks.append({
                    "event_id": str(uuid.uuid4()),
                    "symbol": sym,
                    "timestamp_ms": result.get("t", int(time.time() * 1000)),
                    "price": float(result.get("p", 0)),
                    "volume": int(result.get("s", 0)),
                    "bid_price": None,
                    "ask_price": None,
                    "bid_size": None,
                    "ask_size": None,
                    "exchange": str(result.get("x", "")),
                    "source": "polygon",
                    "is_mock": False,
                })
            except Exception as exc:
                logger.warning("Polygon fetch failed", symbol=sym, error=str(exc))
        return ticks

    def _fetch_alpha_vantage(self) -> list[dict]:
        ticks: list[dict] = []
        base = data_source_config.ALPHA_VANTAGE_BASE_URL
        for sym in self._symbols:
            self._rate_limiter.acquire()
            try:
                resp = self._http.get(
                    base,
                    params={
                        "function": "GLOBAL_QUOTE",
                        "symbol": sym,
                        "apikey": self._av_key,
                    },
                )
                resp.raise_for_status()
                quote = resp.json().get("Global Quote", {})
                ticks.append({
                    "event_id": str(uuid.uuid4()),
                    "symbol": sym,
                    "timestamp_ms": int(time.time() * 1000),
                    "price": float(quote.get("05. price", 0)),
                    "volume": int(quote.get("06. volume", 0)),
                    "bid_price": None,
                    "ask_price": None,
                    "bid_size": None,
                    "ask_size": None,
                    "exchange": None,
                    "source": "alpha_vantage",
                    "is_mock": False,
                })
            except Exception as exc:
                logger.warning("Alpha Vantage fetch failed", symbol=sym, error=str(exc))
        return ticks

    def _fetch_finnhub(self) -> list[dict]:
        ticks: list[dict] = []
        base = data_source_config.FINNHUB_BASE_URL
        for sym in self._symbols:
            self._rate_limiter.acquire()
            try:
                resp = self._http.get(
                    f"{base}/quote",
                    params={"symbol": sym, "token": self._finnhub_key},
                )
                resp.raise_for_status()
                q = resp.json()
                ticks.append({
                    "event_id": str(uuid.uuid4()),
                    "symbol": sym,
                    "timestamp_ms": int(q.get("t", int(time.time()))) * 1000,
                    "price": float(q.get("c", 0)),
                    "volume": 0,
                    "bid_price": None,
                    "ask_price": None,
                    "bid_size": None,
                    "ask_size": None,
                    "exchange": None,
                    "source": "finnhub",
                    "is_mock": False,
                })
            except Exception as exc:
                logger.warning("Finnhub REST fetch failed", symbol=sym, error=str(exc))
        return ticks

    def transform(self, raw_data: list[dict]) -> list[tuple[str | None, bytes]]:
        results: list[tuple[str | None, bytes]] = []
        for record in raw_data:
            vr = validator.validate_market_tick(record)
            if not vr.valid:
                for err in vr.errors:
                    validation_failures_total.labels(
                        producer=self.producer_name, field=err.split()[0]
                    ).inc()
                logger.warning("Invalid tick record", errors=vr.errors)
                continue
            payload = avro_serializer.serialize("market_tick", record)
            if record.get("is_mock"):
                mock_messages_total.labels(producer=self.producer_name).inc()
            results.append((record["symbol"], payload))
        return results
