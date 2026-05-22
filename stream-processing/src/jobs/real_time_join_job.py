from typing import Any

import structlog

from src.config import settings
from src.jobs.base_job import BaseJob
from src.sinks.redis_sink import RedisSink
from src.utils.monitoring import joins_attempted_total, joins_with_sentiment

logger = structlog.get_logger(__name__)


class RealTimeJoinJob(BaseJob):
    """
    Joins 5-min market bars with news and social sentiment caches,
    writing enriched feature vectors to Redis.

    Topology:
        market.bars.5min      ─┐
        news.articles.scored  ─┤─► Redis: finstreami:features:{symbol}
        social.sentiment      ─┘
    """

    SCHEMA_MAP: dict[str, str] = {
        "market.bars.5min": "market_bar",
        "news.articles.scored": "news_sentiment",
        "social.sentiment": "social_sentiment",
    }

    INPUT_TOPICS = ["market.bars.5min", "news.articles.scored", "social.sentiment"]
    OUTPUT_TOPICS: list[str] = []

    def __init__(self, redis_sink: RedisSink | None = None) -> None:
        super().__init__(
            job_name="realtime-join",
            input_topics=self.INPUT_TOPICS,
            output_topics=self.OUTPUT_TOPICS,
            input_schema="market_bar",  # default; overridden by _get_schema
        )
        self._news_cache: dict[str, dict] = {}    # symbol → {score, ts_ms}
        self._social_cache: dict[str, dict] = {}  # symbol → {score, ts_ms}
        self._redis_sink = redis_sink or RedisSink()
        self._join_window_ms = settings.JOIN_WINDOW_MS

    # ─── Schema routing ──────────────────────────────────────────

    def _get_schema(self, msg: Any) -> str:
        return self.SCHEMA_MAP.get(msg.topic(), "market_bar")

    # ─── Processing ──────────────────────────────────────────────

    def process(self, record: dict, raw_msg: Any) -> list:
        topic = raw_msg.topic()
        try:
            if topic == "news.articles.scored":
                self._update_news_cache(record)
            elif topic == "social.sentiment":
                self._update_social_cache(record)
            else:
                self._join_and_write(record)
        except Exception as exc:
            logger.error(
                "RealTimeJoinJob process error",
                topic=topic,
                error=str(exc),
                exc_info=True,
            )
        return []

    def _update_news_cache(self, record: dict) -> None:
        for symbol in record.get("symbols", []):
            entry = {"score": record["sentiment_score"], "ts_ms": record["scored_ms"]}
            existing = self._news_cache.get(symbol)
            if existing is None or record["scored_ms"] >= existing["ts_ms"]:
                self._news_cache[symbol] = entry

    def _update_social_cache(self, record: dict) -> None:
        for symbol in record.get("symbols", []):
            entry = {"score": record["sentiment_score"], "ts_ms": record["scored_ms"]}
            existing = self._social_cache.get(symbol)
            if existing is None or record["scored_ms"] >= existing["ts_ms"]:
                self._social_cache[symbol] = entry

    def _get_recent(
        self, cache: dict[str, dict], symbol: str, bar_ts_ms: int
    ) -> float | None:
        entry = cache.get(symbol)
        if entry is None:
            return None
        if bar_ts_ms - entry["ts_ms"] > self._join_window_ms:
            return None
        return entry["score"]

    def _join_and_write(self, bar: dict) -> None:
        symbol = bar["symbol"]
        bar_ts = bar["timestamp_ms"]

        news_score = self._get_recent(self._news_cache, symbol, bar_ts)
        social_score = self._get_recent(self._social_cache, symbol, bar_ts)

        features: dict = {
            "open": bar.get("open"),
            "high": bar.get("high"),
            "low": bar.get("low"),
            "close": bar.get("close"),
            "volume": bar.get("volume"),
            "vwap": bar.get("vwap", 0.0),
            "trade_count": bar.get("trade_count", 0),
            "news_sentiment": news_score,
            "social_sentiment": social_score,
            "updated_ms": bar_ts,
        }

        self._redis_sink.write_features(symbol, features)
        joins_attempted_total.inc()

        if news_score is not None:
            joins_with_sentiment.labels(source="news").inc()
        if social_score is not None:
            joins_with_sentiment.labels(source="social").inc()

        logger.debug(
            "Bar joined and written to Redis",
            symbol=symbol,
            has_news=news_score is not None,
            has_social=social_score is not None,
        )

    # ─── Lifecycle ───────────────────────────────────────────────

    def run(self) -> None:
        try:
            super().run()
        finally:
            self._redis_sink.close()
