"""PostgreSQL-backed data catalog — tracks partition stats and schema versions."""
from __future__ import annotations

import datetime
import json
from typing import Any

import psycopg2
import psycopg2.extras
import structlog

logger = structlog.get_logger(__name__)

_DDL = """
CREATE TABLE IF NOT EXISTS data_catalog (
    id              SERIAL PRIMARY KEY,
    data_type       TEXT NOT NULL,
    layer           TEXT NOT NULL,
    partition_key   TEXT,
    record_count    BIGINT DEFAULT 0,
    schema_version  INTEGER DEFAULT 1,
    extra           JSONB,
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_catalog_layer_type_partition
    ON data_catalog (layer, data_type, partition_key);
"""


class DataCatalog:
    """Lightweight metadata catalog backed by PostgreSQL."""

    def __init__(self, dsn: str) -> None:
        self._dsn = dsn
        self._conn: psycopg2.extensions.connection | None = None

    # ------------------------------------------------------------------
    # Connection
    # ------------------------------------------------------------------
    def connect(self) -> None:
        self._conn = psycopg2.connect(self._dsn)
        self._conn.autocommit = True
        with self._conn.cursor() as cur:
            cur.execute(_DDL)
        logger.info("DataCatalog connected")

    def close(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None

    def _cursor(self):
        if self._conn is None or self._conn.closed:
            self.connect()
        return self._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # ------------------------------------------------------------------
    # Upsert / update
    # ------------------------------------------------------------------
    def upsert_partition(
        self,
        layer: str,
        data_type: str,
        partition_key: str,
        record_count: int,
        schema_version: int = 1,
        extra: dict[str, Any] | None = None,
    ) -> None:
        sql = """
        INSERT INTO data_catalog (layer, data_type, partition_key, record_count, schema_version, extra, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, NOW())
        ON CONFLICT (layer, data_type, partition_key)
        DO UPDATE SET
            record_count  = EXCLUDED.record_count,
            schema_version = EXCLUDED.schema_version,
            extra          = EXCLUDED.extra,
            updated_at     = NOW();
        """
        with self._cursor() as cur:
            cur.execute(sql, (layer, data_type, partition_key, record_count, schema_version, json.dumps(extra or {})))

    # ------------------------------------------------------------------
    # Query
    # ------------------------------------------------------------------
    def get_partitions(self, layer: str, data_type: str) -> list[dict[str, Any]]:
        sql = "SELECT * FROM data_catalog WHERE layer=%s AND data_type=%s ORDER BY updated_at DESC;"
        with self._cursor() as cur:
            cur.execute(sql, (layer, data_type))
            return [dict(row) for row in cur.fetchall()]

    def get_stats(self) -> list[dict[str, Any]]:
        sql = """
        SELECT layer, data_type, COUNT(*) AS partitions,
               SUM(record_count) AS total_records, MAX(updated_at) AS last_updated
        FROM data_catalog
        GROUP BY layer, data_type
        ORDER BY layer, data_type;
        """
        with self._cursor() as cur:
            cur.execute(sql)
            return [dict(row) for row in cur.fetchall()]

    def list_tables(self) -> list[dict[str, Any]]:
        sql = "SELECT DISTINCT layer, data_type, schema_version FROM data_catalog ORDER BY layer, data_type;"
        with self._cursor() as cur:
            cur.execute(sql)
            return [dict(row) for row in cur.fetchall()]
