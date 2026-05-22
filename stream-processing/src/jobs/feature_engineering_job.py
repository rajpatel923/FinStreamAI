from typing import Any

import structlog

from src.config import settings
from src.jobs.base_job import BaseJob
from src.processors.technical_indicators import TechnicalIndicators
from src.sinks.timescaledb_sink import TimescaleDBSink
from src.state.rolling_state import RollingState

logger = structlog.get_logger(__name__)


class FeatureEngineeringJob(BaseJob):
    """
    Consumes market.bars.1min, computes technical indicators,
    produces to technical.indicators and writes to TimescaleDB.
    """

    INPUT_TOPIC = "market.bars.1min"
    OUTPUT_TOPIC = "technical.indicators"

    def __init__(self, db_sink: TimescaleDBSink | None = None) -> None:
        super().__init__(
            job_name="features",
            input_topics=[self.INPUT_TOPIC],
            output_topics=[self.OUTPUT_TOPIC],
            input_schema="market_bar",
        )
        self._rolling_state = RollingState()
        self._indicators = TechnicalIndicators()
        self._db_sink = db_sink or TimescaleDBSink()

    def process(self, record: dict, raw_msg: Any) -> list[tuple[str, bytes, bytes]]:
        symbol = record["symbol"]
        timeframe = record.get("timeframe", "1min")

        state = self._rolling_state.update_bar(symbol, record)

        if state.bar_count < settings.FEATURE_MIN_BARS:
            logger.debug(
                "Warming up — not enough bars yet",
                symbol=symbol,
                bar_count=state.bar_count,
                min_bars=settings.FEATURE_MIN_BARS,
            )
            return []

        indicators = self._indicators.compute_all(state, symbol, timeframe, record)
        outputs = []
        for ind in indicators:
            key_bytes = symbol.encode()
            value_bytes = self._serializer.serialize("technical_indicator", ind)
            outputs.append((self.OUTPUT_TOPIC, key_bytes, value_bytes))
            try:
                self._db_sink.write_technical_indicator(ind)
            except Exception as exc:
                logger.error("TimescaleDB indicator write failed", job=self.job_name, error=str(exc))

        return outputs

    def run(self) -> None:
        try:
            super().run()
        finally:
            self._db_sink.flush()
