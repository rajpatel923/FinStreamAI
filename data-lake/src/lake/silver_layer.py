"""Silver layer — cleans, deduplicates, and enriches bronze data."""
from __future__ import annotations

import datetime
from typing import Any

import pandas as pd
import structlog

from src.lake.delta_client import DeltaClient

logger = structlog.get_logger(__name__)


class SilverLayer:
    """Reads from bronze, applies transforms, writes to silver."""

    def __init__(
        self,
        delta_client: DeltaClient,
        bronze_base: str,
        silver_base: str,
    ) -> None:
        self._client = delta_client
        self._bronze = bronze_base.rstrip("/")
        self._silver = silver_base.rstrip("/")
        # In-memory watermarks per data type: {type -> last_ingested_at}
        self._watermarks: dict[str, str] = {}

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _read_incremental(self, data_type: str) -> pd.DataFrame:
        path = f"{self._bronze}/{data_type}"
        if not self._client.table_exists(path):
            return pd.DataFrame()
        df = self._client.read(path)
        wm = self._watermarks.get(data_type)
        if wm and "_ingested_at" in df.columns:
            df = df[df["_ingested_at"] > wm]
        return df

    def _update_watermark(self, data_type: str, df: pd.DataFrame) -> None:
        if "_ingested_at" in df.columns and not df.empty:
            self._watermarks[data_type] = df["_ingested_at"].max()

    def _dedup(self, df: pd.DataFrame, key_cols: list[str]) -> pd.DataFrame:
        if df.empty:
            return df
        existing = [c for c in key_cols if c in df.columns]
        if existing:
            df = df.drop_duplicates(subset=existing, keep="last")
        return df

    def _write_silver(self, data_type: str, df: pd.DataFrame, partition_by: list[str]) -> None:
        path = f"{self._silver}/{data_type}"
        self._client.write(path, df, mode="append", partition_by=partition_by)
        logger.info("Silver write", data_type=data_type, rows=len(df))

    # ------------------------------------------------------------------
    # Market data
    # ------------------------------------------------------------------
    def process_market_data(self) -> int:
        df = self._read_incremental("market_tick")
        if df.empty:
            return 0

        # Standardize numeric columns
        for col in ["price", "volume", "bid", "ask"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        # Drop rows with missing price (critical field)
        df = df.dropna(subset=["price"])
        df = self._dedup(df, ["symbol", "timestamp"])

        # Normalize timestamp to ISO string
        if "timestamp" in df.columns:
            df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s", errors="coerce").dt.strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            )

        self._update_watermark("market_tick", df)
        self._write_silver("market_tick", df, partition_by=["symbol", "year", "month", "day"])
        return len(df)

    # ------------------------------------------------------------------
    # News
    # ------------------------------------------------------------------
    def process_news(self) -> int:
        df = self._read_incremental("news_article")
        if df.empty:
            return 0

        df = self._dedup(df, ["article_id"])

        # Drop empty titles
        if "title" in df.columns:
            df = df[df["title"].str.strip().astype(bool)]

        self._update_watermark("news_article", df)
        self._write_silver("news_article", df, partition_by=["year", "month", "day"])
        return len(df)

    # ------------------------------------------------------------------
    # Social
    # ------------------------------------------------------------------
    def process_social(self) -> int:
        df = self._read_incremental("social_post")
        if df.empty:
            return 0

        df = self._dedup(df, ["post_id"])

        # Normalize sentiment score
        if "sentiment_score" in df.columns:
            df["sentiment_score"] = pd.to_numeric(df["sentiment_score"], errors="coerce").clip(-1.0, 1.0)

        self._update_watermark("social_post", df)
        self._write_silver("social_post", df, partition_by=["year", "month", "day"])
        return len(df)
