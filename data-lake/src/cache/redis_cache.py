"""Redis cache — cache-aside + write-through patterns with TTL tiers."""
from __future__ import annotations

import json
from typing import Any, Callable

import redis
import structlog

logger = structlog.get_logger(__name__)

# TTL tiers (seconds)
TTL_PRICES = 60
TTL_FEATURES = 300
TTL_PREDICTIONS = 3600
TTL_SESSIONS = 86400


class RedisCache:
    """Thin Redis wrapper with cache-aside and write-through helpers."""

    def __init__(self, host: str, port: int, password: str | None = None, db: int = 0) -> None:
        self._host = host
        self._port = port
        self._password = password
        self._db = db
        self._client: redis.Redis | None = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    def connect(self) -> None:
        self._client = redis.Redis(
            host=self._host,
            port=self._port,
            password=self._password or None,
            db=self._db,
            decode_responses=True,
        )
        self._client.ping()
        logger.info("RedisCache connected", host=self._host, port=self._port)

    def close(self) -> None:
        if self._client:
            self._client.close()

    @property
    def client(self) -> redis.Redis:
        if self._client is None:
            self.connect()
        return self._client

    # ------------------------------------------------------------------
    # Primitives
    # ------------------------------------------------------------------
    def get(self, key: str) -> Any | None:
        raw = self.client.get(key)
        if raw is None:
            return None
        try:
            return json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            return raw

    def set(self, key: str, value: Any, ttl: int) -> None:
        self.client.setex(key, ttl, json.dumps(value))

    def delete(self, key: str) -> bool:
        return bool(self.client.delete(key))

    def exists(self, key: str) -> bool:
        return bool(self.client.exists(key))

    # ------------------------------------------------------------------
    # Patterns
    # ------------------------------------------------------------------
    def get_or_set(self, key: str, fetch_fn: Callable[[], Any], ttl: int) -> Any:
        """Cache-aside: return cached value or call *fetch_fn* and cache result."""
        cached = self.get(key)
        if cached is not None:
            return cached
        value = fetch_fn()
        if value is not None:
            self.set(key, value, ttl)
        return value

    def write_through(self, key: str, value: Any, ttl: int) -> Any:
        """Write to Redis and return the value (write-through pattern)."""
        self.set(key, value, ttl)
        return value

    # ------------------------------------------------------------------
    # Warm-up
    # ------------------------------------------------------------------
    def warm_prices(self, symbols: list[str], fetch_fn: Callable[[str], Any]) -> int:
        """Pre-load latest prices for *symbols* from the provided fetch function."""
        loaded = 0
        for symbol in symbols:
            try:
                price = fetch_fn(symbol)
                if price is not None:
                    self.set(f"price:{symbol}", price, TTL_PRICES)
                    loaded += 1
            except Exception as exc:
                logger.warning("Price warm-up failed", symbol=symbol, error=str(exc))
        logger.info("Cache warm-up complete", loaded=loaded, total=len(symbols))
        return loaded

    # ------------------------------------------------------------------
    # Stats
    # ------------------------------------------------------------------
    def get_stats(self) -> dict[str, Any]:
        """Return keyspace info (hit/miss rates) from Redis INFO."""
        try:
            info = self.client.info("stats")
            keyspace = self.client.info("keyspace")
            return {
                "hits": info.get("keyspace_hits", 0),
                "misses": info.get("keyspace_misses", 0),
                "total_commands": info.get("total_commands_processed", 0),
                "keyspace": keyspace,
            }
        except Exception as exc:
            logger.error("Failed to get Redis stats", error=str(exc))
            return {}
