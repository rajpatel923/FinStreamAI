from typing import Any

from src.jobs.base_job import BaseJob
from src.processors.data_cleaner import DataCleaner
from src.state.rolling_state import RollingState


class DataCleaningJob(BaseJob):
    """Consumes market.ticks.raw, validates/deduplicates/tags, produces to market.ticks.clean."""

    INPUT_TOPIC = "market.ticks.raw"
    OUTPUT_TOPIC = "market.ticks.clean"

    def __init__(self) -> None:
        super().__init__(
            job_name="cleaner",
            input_topics=[self.INPUT_TOPIC],
            output_topics=[self.OUTPUT_TOPIC],
            input_schema="market_tick",
        )
        self._rolling_state = RollingState()
        self._cleaner = DataCleaner(self._rolling_state)

    def process(self, record: dict, raw_msg: Any) -> list[tuple[str, bytes, bytes]]:
        cleaned = self._cleaner.clean(record)
        if cleaned is None:
            return []

        # Remove internal cleaner tags before re-serializing so schema stays clean.
        # The _is_outlier / _zscore fields are for downstream jobs via metadata.
        is_outlier = cleaned.pop("_is_outlier", False)
        zscore = cleaned.pop("_zscore", None)

        # Re-use the existing market_tick schema; outlier info is dropped here
        # (downstream consumers get unmodified ticks; anomaly detection runs separately).
        key_bytes = cleaned["symbol"].encode()
        value_bytes = self._serializer.serialize("market_tick", cleaned)
        return [(self.OUTPUT_TOPIC, key_bytes, value_bytes)]
