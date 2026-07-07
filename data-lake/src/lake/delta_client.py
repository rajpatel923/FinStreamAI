"""Delta Lake client — thin wrapper around delta-rs (deltalake package)."""
from __future__ import annotations

from typing import Any

import pandas as pd
import structlog
from deltalake import DeltaTable, write_deltalake

logger = structlog.get_logger(__name__)


class DeltaClient:
    """Provides read/write helpers for Delta tables on MinIO/S3."""

    def __init__(self, storage_options: dict[str, str]) -> None:
        self.storage_options = storage_options

    # ------------------------------------------------------------------
    # Write
    # ------------------------------------------------------------------
    def write(
        self,
        path: str,
        df: pd.DataFrame,
        mode: str = "append",
        partition_by: list[str] | None = None,
        schema_mode: str = "merge",
        schema: Any | None = None,
    ) -> None:
        """Write a DataFrame to a Delta table.

        Args:
            path: Full S3/MinIO path, e.g. ``s3://bucket/bronze/market_ticks``
            df: Data to write.
            mode: ``append`` | ``overwrite``
            partition_by: List of column names for partitioning.
            schema_mode: ``merge`` | ``overwrite`` — how to handle schema evolution.
            schema: Optional explicit pyarrow schema. Needed for record types with
                nullable fields — without it, a row where such a field is None makes
                pyarrow infer that column as null-type, which Delta Lake rejects.
        """
        if df.empty:
            logger.debug("Skipping write — empty DataFrame", path=path)
            return

        write_deltalake(
            path,
            df,
            schema=schema,
            mode=mode,
            partition_by=partition_by,
            overwrite_schema=(schema_mode == "overwrite"),
            storage_options=self.storage_options,
        )
        logger.debug("Delta write complete", path=path, rows=len(df), mode=mode)

    # ------------------------------------------------------------------
    # Read
    # ------------------------------------------------------------------
    def read(self, path: str, version: int | None = None) -> pd.DataFrame:
        """Read a Delta table into a DataFrame.

        Supports time-travel via ``version``.
        """
        dt = DeltaTable(path, storage_options=self.storage_options, version=version)
        df = dt.to_pandas()
        logger.debug("Delta read complete", path=path, rows=len(df), version=version)
        return df

    def read_filtered(
        self,
        path: str,
        filters: list[tuple[str, str, Any]],
        version: int | None = None,
    ) -> pd.DataFrame:
        """Read with partition / row-group filters (push-down)."""
        dt = DeltaTable(path, storage_options=self.storage_options, version=version)
        df = dt.to_pandas(filters=filters)
        logger.debug("Delta filtered read", path=path, rows=len(df), filters=filters)
        return df

    # ------------------------------------------------------------------
    # Maintenance
    # ------------------------------------------------------------------
    def vacuum(self, path: str, retention_hours: int = 168) -> None:
        """Remove files older than ``retention_hours`` from a Delta table."""
        dt = DeltaTable(path, storage_options=self.storage_options)
        dt.vacuum(retention_hours=retention_hours, dry_run=False, enforce_retention_duration=False)
        logger.info("Delta vacuum complete", path=path, retention_hours=retention_hours)

    def get_history(self, path: str, limit: int = 10) -> list[dict]:
        """Return the last N commits from the Delta log."""
        dt = DeltaTable(path, storage_options=self.storage_options)
        return dt.history(limit=limit)

    def table_exists(self, path: str) -> bool:
        """Return True if a Delta table exists at *path*."""
        try:
            DeltaTable(path, storage_options=self.storage_options)
            return True
        except Exception:
            return False
