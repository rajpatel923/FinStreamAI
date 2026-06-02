from __future__ import annotations

import structlog
import pandas as pd

from src.config import settings

logger = structlog.get_logger(__name__)


class FeatureLoader:
    """Loads feature vectors from Redis and historical OHLCV bars from TimescaleDB."""

    def __init__(self) -> None:
        self._redis = None

    def _get_redis(self):
        if self._redis is None:
            import redis as redis_lib

            self._redis = redis_lib.from_url(settings.redis_url, decode_responses=True)
        return self._redis

    def get_latest_features(self, symbol: str) -> dict | None:
        """Pull the latest enriched feature vector from Redis.

        Keys are written by RealTimeJoinJob under finstreami:features:{symbol}.
        Returns None if no features exist for the symbol.
        """
        r = self._get_redis()
        data = r.hgetall(f"finstreami:features:{symbol}")
        if not data:
            return None
        result: dict = {}
        for k, v in data.items():
            try:
                result[k] = float(v)
            except (ValueError, TypeError):
                result[k] = v
        return result

    def get_historical_bars(self, symbol: str, limit: int = 500) -> pd.DataFrame:
        """Load OHLCV bars from TimescaleDB for model training.

        Returns empty DataFrame if the DB is unavailable or has no data.
        Column names: timestamp, open, high, low, close, volume, vwap.
        """
        import psycopg2
        import psycopg2.extras

        try:
            with psycopg2.connect(settings.timescaledb_sync_url) as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                    cur.execute(
                        """
                        SELECT time        AS timestamp,
                               open_price  AS open,
                               high_price  AS high,
                               low_price   AS low,
                               close_price AS close,
                               volume,
                               vwap
                        FROM market_bars
                        WHERE symbol = %s
                        ORDER BY time DESC
                        LIMIT %s
                        """,
                        (symbol, limit),
                    )
                    rows = cur.fetchall()
        except Exception as exc:
            logger.warning("TimescaleDB unavailable for feature loading", error=str(exc))
            return pd.DataFrame()

        if not rows:
            return pd.DataFrame()

        df = pd.DataFrame(
            [dict(r) for r in rows],
            columns=["timestamp", "open", "high", "low", "close", "volume", "vwap"],
        )
        return df.sort_values("timestamp").reset_index(drop=True)
