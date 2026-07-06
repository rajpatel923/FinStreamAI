"""Bronze layer — writes raw Kafka records to Delta tables on MinIO."""
from __future__ import annotations

import datetime
from typing import Any

import pandas as pd
import pyarrow as pa
import structlog

from src.lake.delta_client import DeltaClient
from src.quality.quarantine import Quarantine

logger = structlog.get_logger(__name__)

_REQUIRED_FIELDS: dict[str, list[str]] = {
    "market_tick": ["symbol", "price", "timestamp_ms"],
    "news_article": ["article_id", "headline", "published_ms"],
    "social_post": ["post_id", "body", "created_ms"],
    "event": ["source_id", "event_type", "extracted_ms"],
}

# ms-epoch field per record_type used to derive the partition timestamp.
_TIMESTAMP_FIELD: dict[str, str] = {
    "market_tick": "timestamp_ms",
    "news_article": "published_ms",
    "social_post": "created_ms",
    "event": "extracted_ms",
}

# "event" records come from Claude/spaCy extraction as plain JSON, where
# "date" and "error" are legitimately absent/None depending on which
# extraction path ran. An explicit schema keeps those columns stably typed
# across writes instead of drifting per-row.
_EVENT_SCHEMA = pa.schema(
    [
        ("source_id", pa.string()),
        ("extracted_ms", pa.int64()),
        ("event_type", pa.string()),
        ("companies", pa.list_(pa.string())),
        ("date", pa.string()),
        ("confidence", pa.float64()),
        ("summary", pa.string()),
        ("error", pa.string()),
        ("_ingested_at", pa.string()),
        ("_source_system", pa.string()),
        ("year", pa.int64()),
        ("month", pa.int64()),
        ("day", pa.int64()),
        ("hour", pa.int64()),
    ]
)

# social_post.symbols can be an empty list [] for posts that don't mention any
# ticker. PyArrow infers [] as list<null> without an explicit schema, which
# conflicts with an existing Delta table typed list<string>. The schema below
# pins the type so empty-list rows are written correctly.
_SOCIAL_POST_SCHEMA = pa.schema(
    [
        ("post_id", pa.string()),
        ("platform", pa.string()),
        ("subreddit", pa.string()),
        ("title", pa.string()),
        ("body", pa.string()),
        ("author", pa.string()),
        ("score", pa.int64()),
        ("num_comments", pa.int64()),
        ("created_ms", pa.int64()),
        ("symbols", pa.list_(pa.string())),
        ("url", pa.string()),
        ("is_mock", pa.bool_()),
        ("_ingested_at", pa.string()),
        ("_source_system", pa.string()),
        ("year", pa.int64()),
        ("month", pa.int64()),
        ("day", pa.int64()),
        ("hour", pa.int64()),
    ]
)

_SCHEMAS: dict[str, pa.Schema] = {"event": _EVENT_SCHEMA, "social_post": _SOCIAL_POST_SCHEMA}


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
        ts_ms = enriched.get(_TIMESTAMP_FIELD.get(record_type, ""))
        ts_seconds = ts_ms / 1000 if isinstance(ts_ms, (int, float)) else ts_ms
        parts = self._time_parts(ts_seconds)
        enriched.update(parts)

        schema = _SCHEMAS.get(record_type)
        if schema is not None:
            # Fill any fields absent from this particular record so every
            # write has the same column set, then let the explicit schema
            # (rather than pyarrow's inference) type the None values.
            row = {field.name: enriched.get(field.name) for field in schema}
            df = pd.DataFrame([row])
        else:
            # A single-row DataFrame with a None value infers that column as
            # pyarrow null-type, which Delta Lake rejects outright.
            df = pd.DataFrame([{k: v for k, v in enriched.items() if v is not None}])
        path = f"{self._base}/{record_type}"
        self._client.write(path, df, mode="append", partition_by=partition_by, schema=schema)

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
