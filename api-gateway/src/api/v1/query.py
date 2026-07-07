"""Query endpoints: market data, sentiment, custom (premium)."""
from __future__ import annotations

from datetime import datetime
from typing import Annotated

import structlog
from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.config import settings
from src.core.database import get_timescale_db
from src.core.dependencies import get_current_user, require_role
from src.models.user import User
from src.schemas.query import CustomQueryRequest, CustomQueryResponse, MarketDataResponse, SentimentResponse
from src.services import query_service

logger = structlog.get_logger(__name__)
router = APIRouter(prefix="/query", tags=["query"])


@router.get("/market-data", response_model=MarketDataResponse)
async def get_market_data(
    symbol: str = Query(..., max_length=20),
    interval: str = Query(default="1h", max_length=10),
    from_ts: datetime | None = None,
    to_ts: datetime | None = None,
    limit: int = Query(default=1000, ge=1, le=10000),
    cursor: str | None = None,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_timescale_db),
):
    hours_limit = None

    if user.role == "free_user":
        hours_limit = settings.FREE_TIER_MARKET_DATA_HOURS
        if from_ts is None:
            from datetime import timezone, timedelta
            now = datetime.now(timezone.utc)
            from_ts = now - timedelta(hours=hours_limit)
        limit = min(limit, 5000)

    result = await query_service.query_market_data(
        db, symbol, from_ts, to_ts, limit, cursor, hours_limit, interval
    )
    return MarketDataResponse(**result)


@router.get("/sentiment", response_model=SentimentResponse)
async def get_sentiment(
    symbol: str = Query(default=None, max_length=20),
    from_ts: datetime | None = None,
    to_ts: datetime | None = None,
    limit: int = Query(default=500, ge=1, le=5000),
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_timescale_db),
):
    if user.role == "free_user":
        limit = min(limit, 100)
    result = await query_service.query_sentiment(db, symbol, from_ts, to_ts, limit)
    return SentimentResponse(**result)


@router.post("/custom", response_model=CustomQueryResponse)
async def custom_query(
    req: CustomQueryRequest,
    user: User = Depends(require_role("premium_user", "admin")),
    db: AsyncSession = Depends(get_timescale_db),
):
    try:
        result = await query_service.run_custom_query(
            db,
            table=req.table,
            symbol=req.symbol,
            from_ts=req.from_ts,
            to_ts=req.to_ts,
            limit=req.limit,
            filters=req.filters,
            order_by=req.order_by,
            order_dir=req.order_dir,
        )
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(e))
    return CustomQueryResponse(**result)
