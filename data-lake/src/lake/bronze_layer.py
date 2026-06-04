"""Bronze layer — writes raw Kafka records to Delta tables on MinIO."""
from __future__ import annotations

import datetime
from typing import Any

import pandas as pd
import structlog

from src.lake.delta_client import DeltaClient
from src.quality.quarantine import Quarantine

logger = structlog.get_logger(__name__)

_REQUIRED_FIELDS: dict[str, list[str]] = {
    "market_tick": ["symbol", "price", "timestamp"],
    "news_article": ["article_id", "title", "timestamp"],
    "social_post": ["post_id", "content", "timestamp"],
    "event": ["event_id", "event_type", "timestamp"],
}


class BronzeLayer:
    """Writes raw records to Delta tables partitioned by time."""

    def __init__(self, delta_client: DeltaClient, base_path: str, quarantine: Quarantine) -> None:
        self._client = delta_client
        self._base = base_path.rstrip("/")
        self._quarantine = quarantine

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _validate(self, record_type: str, record: dict[str, Any]) -> bool:
        required = _REQUIRED_FIELDS.get(record_type, [])
        return all(record.get(f) is not None for f in required)

    @staticmethod
    def _time_parts(ts: Any) -> dict[str, int]:
        if isinstance(ts, (int, float)):
            dt = datetime.datetime.utcfromtimestamp(ts)
        elif isinstance(ts, str):
            dt = datetime.datetime.fromisoformat(ts)
        elif isinstance(ts, datetime.datetime):
            dt = ts
        else:
            dt = datetime.datetime.utcnow()
        return {"year": dt.year, "month": dt.month, "day": dt.day, "hour": dt.hour}

    def _enrich(self, record: dict[str, Any]) -> dict[str, Any]:
        record = dict(record)
        record["_ingested_at"] = datetime.datetime.utcnow().isoformat()
        record["_source_system"] = "kafka"
        return record

    def _write(
        self,
        record_type: str,
        record: dict[str, Any],
        partition_by: list[str],
    ) -> None:
        if not self._validate(record_type, record):
            logger.warning("Validation failed — routing to quarantine", record_type=record_type)
            self._quarantine.store(record_type, record, reason="missing_required_fields")
            return

        enriched = self._enrich(record)
        parts = self._time_parts(enriched.get("timestamp"))
        enriched.update(parts)

        df = pd.DataFrame([enriched])
        path = f"{self._base}/{record_type}"
        self._client.write(path, df, mode="append", partition_by=partition_by)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def write_market_tick(self, record: dict[str, Any]) -> None:
        self._write(
            "market_tick",
            record,
            partition_by=["symbol", "year", "month", "day", "hour"],
        )

    def write_news_article(self, record: dict[str, Any]) -> None:
        self._write(
            "news_article",
            record,
            partition_by=["year", "month", "day", "hour"],
        )

    def write_social_post(self, record: dict[str, Any]) -> None:
        self._write(
            "social_post",
            record,
            partition_by=["year", "month", "day", "hour"],
        )

    def write_event(self, record: dict[str, Any]) -> None:
        self._write(
            "event",
            record,
            partition_by=["year", "month", "day"],
        )
