import time

import redis
import structlog

from src.config import settings
from src.utils.monitoring import (
    redis_write_duration_seconds,
    redis_write_errors_total,
    redis_writes_total,
)

logger = structlog.get_logger(__name__)

_FEATURES_KEY_PREFIX = "finstreami:features:"
_SENTIMENT_KEY_PREFIX = "finstreami:sentiment:"


class RedisSink:
    """Sync Redis writer for the feature store (HSET per symbol)."""

    def __init__(
        self,
        host: str | None = None,
        port: int | None = None,
        password: str | None = None,
        ttl_s: int | None = None,
    ) -> None:
        self._host = host or settings.REDIS_HOST
        self._port = port or settings.REDIS_PORT
        self._password = password or settings.REDIS_PASSWORD or None
        self._ttl_s = ttl_s if ttl_s is not None else settings.FEATURE_STORE_TTL_S
        self._conn: redis.Redis | None = None

    def _get_conn(self) -> redis.Redis:
        if self._conn is None:
            self._conn = redis.Redis(
                host=self._host,
                port=self._port,
                password=self._password,
                decode_responses=False,
            )
            logger.info("RedisSink connected", host=self._host, port=self._port)
        return self._conn

    def write_features(self, symbol: str, features: dict) -> None:
        """Write feature hash to finstreami:features:{symbol} with TTL."""
        filtered = {k: v for k, v in features.items() if v is not None}
        if not filtered:
            return
        key = f"{_FEATURES_KEY_PREFIX}{symbol}"
        start = time.perf_counter()
        try:
            conn = self._get_conn()
            conn.hset(key, mapping={k: str(v) for k, v in filtered.items()})
            conn.expire(key, self._ttl_s)
            redis_writes_total.labels(type="features").inc()
        except redis.RedisError as exc:
            logger.error("RedisSink write_features failed", symbol=symbol, error=str(exc))
            redis_write_errors_total.labels(type="features").inc()
        finally:
            redis_write_duration_seconds.labels(type="features").observe(
                time.perf_counter() - start
            )

    def write_sentiment(self, symbol: str, sentiment: dict) -> None:
        """Write sentiment hash to finstreami:sentiment:{symbol} with TTL."""
        filtered = {k: v for k, v in sentiment.items() if v is not None}
        if not filtered:
            return
        key = f"{_SENTIMENT_KEY_PREFIX}{symbol}"
        start = time.perf_counter()
        try:
            conn = self._get_conn()
            conn.hset(key, mapping={k: str(v) for k, v in filtered.items()})
            conn.expire(key, self._ttl_s)
            redis_writes_total.labels(type="sentiment").inc()
        except redis.RedisError as exc:
            logger.error("RedisSink write_sentiment failed", symbol=symbol, error=str(exc))
            redis_write_errors_total.labels(type="sentiment").inc()
        finally:
            redis_write_duration_seconds.labels(type="sentiment").observe(
                time.perf_counter() - start
            )

    def flush(self) -> None:
        """No-op — Redis client is stateless per call."""

    def close(self) -> None:
        """Close the underlying connection if open."""
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None
