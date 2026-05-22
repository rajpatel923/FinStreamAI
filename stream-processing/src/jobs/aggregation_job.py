import time
from typing import Any

import structlog

from src.jobs.base_job import BaseJob
from src.processors.tick_aggregator import TickAggregator
from src.sinks.timescaledb_sink import TimescaleDBSink
from src.state.window_state import WindowState
from src.utils.monitoring import window_buffers_active

logger = structlog.get_logger(__name__)

_TIMEFRAMES = ["5min", "15min", "1hour"]
_TOPIC_MAP = {
    "5min": "market.bars.5min",
    "15min": "market.bars.15min",
    "1hour": "market.bars.1hour",
}


class AggregationJob(BaseJob):
    """
    Consumes market.ticks.clean, produces OHLCV bars to market.bars.{5min,15min,1hour}
    and writes to TimescaleDB market_bars hypertable.
    Note: market.bars.1min is produced by Phase 2 MarketBarProducer — not here.
    """

    INPUT_TOPIC = "market.ticks.clean"

    def __init__(self, db_sink: TimescaleDBSink | None = None) -> None:
        super().__init__(
            job_name="aggregator",
            input_topics=[self.INPUT_TOPIC],
            output_topics=list(_TOPIC_MAP.values()),
            input_schema="market_tick",
        )
        self._window_state = WindowState()
        self._aggregator = TickAggregator(self._window_state)
        self._db_sink = db_sink or TimescaleDBSink()

    def process(self, record: dict, raw_msg: Any) -> list[tuple[str, bytes, bytes]]:
        self._aggregator.ingest(record)
        # Update active window gauge
        window_buffers_active.labels(job=self.job_name, timeframe="all").set(
            self._aggregator.active_window_count()
        )
        return []  # outputs emitted in _flush_pending

    def _flush_pending(self, now_ms: int) -> None:
        bars = self._aggregator.get_closeable_bars(now_ms)
        for bar in bars:
            timeframe = bar["timeframe"]
            topic = _TOPIC_MAP.get(timeframe)
            if topic is None:
                continue
            key_bytes = bar["symbol"].encode()
            value_bytes = self._serializer.serialize("market_bar", bar)
            self._sink.produce(topic, key_bytes, value_bytes)
            try:
                self._db_sink.write_market_bar(bar)
            except Exception as exc:
                logger.error("TimescaleDB write failed", job=self.job_name, error=str(exc))

    def run(self) -> None:
        try:
            super().run()
        finally:
            self._db_sink.flush()
