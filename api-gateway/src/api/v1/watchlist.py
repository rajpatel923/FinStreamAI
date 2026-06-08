"""Watchlist CRUD endpoints."""
from __future__ import annotations

from typing import Annotated

import structlog
from fastapi import APIRouter, Depends, status
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.database import get_db
from src.core.dependencies import get_current_user, get_redis
from src.models.user import User
from src.schemas.watchlist import WatchlistItemCreate, WatchlistItemResponse
from src.services import watchlist_service

logger = structlog.get_logger(__name__)
router = APIRouter(prefix="/watchlist", tags=["watchlist"])


@router.post("", response_model=WatchlistItemResponse, status_code=status.HTTP_201_CREATED)
async def add_item(
    req: WatchlistItemCreate,
    user: Annotated[User, Depends(get_current_user)],
    db: Annotated[AsyncSession, Depends(get_db)],
):
    redis = get_redis()
    return await watchlist_service.add_watchlist_item(user.id, req, db, user.role, redis)


@router.get("", response_model=list[WatchlistItemResponse])
async def list_items(
    user: Annotated[User, Depends(get_current_user)],
    db: Annotated[AsyncSession, Depends(get_db)],
):
    return await watchlist_service.list_watchlist(user.id, db)


@router.delete("/{symbol}", status_code=status.HTTP_204_NO_CONTENT)
async def remove_item(
    symbol: str,
    user: Annotated[User, Depends(get_current_user)],
    db: Annotated[AsyncSession, Depends(get_db)],
):
    redis = get_redis()
    await watchlist_service.remove_watchlist_item(user.id, symbol, db, redis)
