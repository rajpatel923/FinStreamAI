"""Pydantic schemas for query and export endpoints."""
from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


class MarketDataPoint(BaseModel):
    timestamp: datetime
    symbol: str
    open: float | None = None
    high: float | None = None
    low: float | None = None
    close: float | None = None
    volume: float | None = None
    vwap: float | None = None


class MarketDataResponse(BaseModel):
    symbol: str
    data: list[MarketDataPoint]
    cursor: str | None = None
    total_count: int | None = None


class SentimentPoint(BaseModel):
    timestamp: datetime
    symbol: str | None
    sentiment_score: float
    sentiment_label: str
    source: str | None = None


class SentimentResponse(BaseModel):
    data: list[SentimentPoint]
    cursor: str | None = None


class CustomQueryRequest(BaseModel):
    """Premium-only parameterized query builder."""

    table: str = Field(description="Target table name")
    symbol: str | None = None
    from_ts: datetime | None = None
    to_ts: datetime | None = None
    limit: int = Field(default=1000, ge=1, le=10000)
    filters: dict[str, Any] = Field(default_factory=dict)
    order_by: str = "timestamp"
    order_dir: str = "desc"


class CustomQueryResponse(BaseModel):
    rows: list[dict[str, Any]]
    cursor: str | None = None
    row_count: int


class ExportRequest(BaseModel):
    query_params: dict[str, Any]
    output_format: str = Field(default="json", pattern="^(json|csv|parquet)$")


class ExportJobResponse(BaseModel):
    id: uuid.UUID
    status: str
    output_format: str
    row_count: int | None = None
    file_size_bytes: int | None = None
    download_url: str | None = None
    error_message: str | None = None
    created_at: datetime
    completed_at: datetime | None = None

    model_config = {"from_attributes": True}
