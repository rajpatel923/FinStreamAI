import time
import uuid
from typing import Any

import httpx
import structlog

from src.config.data_source_config import data_source_config
from src.producers.base_producer import BaseProducer
from src.schemas.registry import avro_serializer
from src.utils.data_validation import validator
from src.utils.mock_data import mock_generator
from src.utils.monitoring import mock_messages_total, validation_failures_total
from src.utils.rate_limiter import TokenBucket

logger = structlog.get_logger(__name__)


class MarketBarProducer(BaseProducer):
    """Produces 1-minute OHLCV bars to market.bars.1min."""

    def __init__(self) -> None:
        super().__init__("market_bar_producer", "market.bars.1min")
        cfg = data_source_config
        self._av_key = cfg.ALPHA_VANTAGE_API_KEY
        self._use_mock = cfg.USE_MOCK_DATA or not self._av_key
        self._symbols = cfg.watched_symbols_list
        self._rate_limiter = TokenBucket(rate_per_minute=cfg.ALPHA_VANTAGE_RATE_LIMIT)
        self._http = httpx.Client(timeout=cfg.HTTP_TIMEOUT_SECONDS)

        source = "mock" if self._use_mock else "alpha_vantage"
        logger.info("MarketBarProducer initialised", source=source, symbols=self._symbols)

    def data_source_name(self) -> str:
        return "mock" if self._use_mock else "alpha_vantage"

    def poll_interval_seconds(self) -> float:
        return 60.0

    def fetch_data(self) -> list[dict]:
        if self._use_mock:
            return [mock_generator.market_bar(sym) for sym in self._symbols]
        return self._fetch_alpha_vantage()

    def _fetch_alpha_vantage(self) -> list[dict]:
        bars: list[dict] = []
        base = data_source_config.ALPHA_VANTAGE_BASE_URL
        for sym in self._symbols:
            self._rate_limiter.acquire()
            try:
                resp = self._http.get(
                    base,
                    params={
                        "function": "TIME_SERIES_INTRADAY",
                        "symbol": sym,
                        "interval": "1min",
                        "outputsize": "compact",
                        "apikey": self._av_key,
                    },
                )
                resp.raise_for_status()
                ts_data = resp.json().get("Time Series (1min)", {})
                if not ts_data:
                    continue
                latest_key = max(ts_data.keys())
                bar = ts_data[latest_key]
                bars.append({
                    "bar_id": str(uuid.uuid4()),
                    "symbol": sym,
                    "timeframe": "1min",
                    "timestamp_ms": int(time.time() * 1000),
                    "open": float(bar.get("1. open", 0)),
                    "high": float(bar.get("2. high", 0)),
                    "low": float(bar.get("3. low", 0)),
                    "close": float(bar.get("4. close", 0)),
                    "volume": int(bar.get("5. volume", 0)),
                    "vwap": None,
                    "trade_count": None,
                    "source": "alpha_vantage",
                    "is_mock": False,
                })
            except Exception as exc:
                logger.warning("Alpha Vantage bar fetch failed", symbol=sym, error=str(exc))
        return bars

    def transform(self, raw_data: list[dict]) -> list[tuple[str | None, bytes]]:
        results: list[tuple[str | None, bytes]] = []
        for record in raw_data:
            # Basic bar validation (price fields > 0)
            errors = []
            for field in ("open", "high", "low", "close"):
                val = record.get(field, 0)
                if not isinstance(val, (int, float)) or val <= 0:
                    errors.append(f"{field} must be > 0")
            if errors:
                for err in errors:
                    validation_failures_total.labels(
                        producer=self.producer_name, field=err.split()[0]
                    ).inc()
                logger.warning("Invalid bar record", errors=errors)
                continue
            payload = avro_serializer.serialize("market_bar", record)
            if record.get("is_mock"):
                mock_messages_total.labels(producer=self.producer_name).inc()
            results.append((record["symbol"], payload))
        return results
