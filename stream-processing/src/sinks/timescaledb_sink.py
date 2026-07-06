import time
from datetime import datetime, timezone

import psycopg2
import structlog

from src.config import settings
from src.utils.monitoring import timescaledb_write_duration_seconds, timescaledb_writes_total

logger = structlog.get_logger(__name__)

_BATCH_SIZE = 100


class TimescaleDBSink:
    """Batched psycopg2 writer with ON CONFLICT DO NOTHING semantics."""

    def __init__(self, dsn: str | None = None, batch_size: int = _BATCH_SIZE) -> None:
        self._dsn = dsn or settings.timescaledb_sync_url
        self._batch_size = batch_size
        self._conn: psycopg2.extensions.connection | None = None
        self._bar_buffer: list[dict] = []
        self._indicator_buffer: list[dict] = []
        self._signal_buffer: list[dict] = []

    def _get_conn(self) -> psycopg2.extensions.connection:
        if self._conn is None or self._conn.closed:
            self._conn = psycopg2.connect(self._dsn)
            self._conn.autocommit = False
            logger.info("TimescaleDBSink connected")
        return self._conn

    # ─── Market bars ────────────────────────────────────────────
    def write_market_bar(self, bar: dict) -> None:
        self._bar_buffer.append(bar)
        if len(self._bar_buffer) >= self._batch_size:
            self._flush_bars()

    def _flush_bars(self) -> None:
        if not self._bar_buffer:
            return
        start = time.perf_counter()
        conn = self._get_conn()
        try:
            with conn.cursor() as cur:
                for bar in self._bar_buffer:
                    ts = datetime.fromtimestamp(bar["timestamp_ms"] / 1000, tz=timezone.utc)
                    cur.execute(
                        """
                        INSERT INTO market_bars
                            (time, symbol, timeframe, open_price, high_price, low_price, close_price, volume, vwap, trade_count)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (time, symbol, timeframe) DO NOTHING
                        """,
                        (
                            ts,
                            bar["symbol"],
                            bar["timeframe"],
                            bar["open"],
                            bar["high"],
                            bar["low"],
                            bar["close"],
                            bar["volume"],
                            bar.get("vwap"),
                            bar.get("trade_count"),
                        ),
                    )
            conn.commit()
            count = len(self._bar_buffer)
            timescaledb_writes_total.labels(table="market_bars").inc(count)
        except Exception as exc:
            conn.rollback()
            logger.error("Failed to flush market bars", error=str(exc))
            self._conn = None
            raise
        finally:
            timescaledb_write_duration_seconds.labels(table="market_bars").observe(
                time.perf_counter() - start
            )
            self._bar_buffer.clear()

    # ─── Technical indicators ────────────────────────────────────
    def write_technical_indicator(self, ind: dict) -> None:
        self._indicator_buffer.append(ind)
        if len(self._indicator_buffer) >= self._batch_size:
            self._flush_indicators()

    def _flush_indicators(self) -> None:
        if not self._indicator_buffer:
            return
        start = time.perf_counter()
        conn = self._get_conn()
        try:
            with conn.cursor() as cur:
                for ind in self._indicator_buffer:
                    ts = datetime.fromtimestamp(ind["timestamp_ms"] / 1000, tz=timezone.utc)
                    import json
                    meta = {**ind.get("metadata", {})}
                    for extra_key in ("signal_value", "upper_band", "lower_band"):
                        if ind.get(extra_key) is not None:
                            meta[extra_key] = ind[extra_key]
                    cur.execute(
                        """
                        INSERT INTO technical_indicators
                            (time, symbol, indicator_name, timeframe, value, metadata)
                        VALUES (%s, %s, %s, %s, %s, %s::jsonb)
                        ON CONFLICT (time, symbol, indicator_name, timeframe) DO NOTHING
                        """,
                        (
                            ts,
                            ind["symbol"],
                            ind["indicator_name"],
                            ind["timeframe"],
                            ind["value"],
                            json.dumps(meta),
                        ),
                    )
            conn.commit()
            timescaledb_writes_total.labels(table="technical_indicators").inc(
                len(self._indicator_buffer)
            )
        except Exception as exc:
            conn.rollback()
            logger.error("Failed to flush indicators", error=str(exc))
            self._conn = None
            raise
        finally:
            timescaledb_write_duration_seconds.labels(table="technical_indicators").observe(
                time.perf_counter() - start
            )
            self._indicator_buffer.clear()

    # ─── Trading signals ─────────────────────────────────────────
    def write_trading_signal(self, sig: dict) -> None:
        self._signal_buffer.append(sig)
        if len(self._signal_buffer) >= self._batch_size:
            self._flush_signals()

    def _flush_signals(self) -> None:
        if not self._signal_buffer:
            return
        start = time.perf_counter()
        conn = self._get_conn()
        try:
            with conn.cursor() as cur:
                for sig in self._signal_buffer:
                    ts = datetime.fromtimestamp(sig["timestamp_ms"] / 1000, tz=timezone.utc)
                    meta = __import__("json").dumps({
                        "indicator_values": sig.get("indicator_values", {}),
                        "source": sig.get("source", "signal-generation"),
                    })
                    cur.execute(
                        """
                        INSERT INTO trading_signals
                            (time, symbol, signal_type, signal_strength, direction, confidence, metadata)
                        VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb)
                        ON CONFLICT (time, symbol, signal_type) DO NOTHING
                        """,
                        (
                            ts,
                            sig["symbol"],
                            sig["signal_type"],
                            sig["strength"],
                            sig["direction"],
                            sig.get("confidence", sig.get("strength", 0.0)),
                            meta,
                        ),
                    )
            conn.commit()
            timescaledb_writes_total.labels(table="trading_signals").inc(len(self._signal_buffer))
        except Exception as exc:
            conn.rollback()
            logger.error("Failed to flush signals", error=str(exc))
            self._conn = None
            raise
        finally:
            timescaledb_write_duration_seconds.labels(table="trading_signals").observe(
                time.perf_counter() - start
            )
            self._signal_buffer.clear()

    # ─── Flush all buffers ───────────────────────────────────────
    def flush(self) -> None:
        self._flush_bars()
        self._flush_indicators()
        self._flush_signals()

    def close(self) -> None:
        try:
            self.flush()
        finally:
            if self._conn and not self._conn.closed:
                self._conn.close()
            self._conn = None
