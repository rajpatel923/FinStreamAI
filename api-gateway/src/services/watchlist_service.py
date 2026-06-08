"""Watchlist and UserPreference CRUD."""
from __future__ import annotations

import uuid

import structlog
from fastapi import HTTPException, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.config import settings
from src.models.watchlist import UserPreference, WatchlistItem
from src.schemas.watchlist import UserPreferenceUpdate, WatchlistItemCreate

logger = structlog.get_logger(__name__)


# ─── Watchlist ────────────────────────────────────────────────────────────────

async def add_watchlist_item(
    user_id: uuid.UUID,
    req: WatchlistItemCreate,
    db: AsyncSession,
    user_role: str,
    redis=None,
) -> WatchlistItem:
    # Free-tier cap
    if user_role == "free_user":
        result = await db.execute(
            select(WatchlistItem).where(WatchlistItem.user_id == user_id)
        )
        existing = result.scalars().all()
        if len(existing) >= settings.FREE_TIER_MAX_WATCHLIST:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Free tier limited to {settings.FREE_TIER_MAX_WATCHLIST} watchlist symbols",
            )

    # Duplicate check
    dup = await db.execute(
        select(WatchlistItem).where(
            WatchlistItem.user_id == user_id,
            WatchlistItem.symbol == req.symbol,
        )
    )
    if dup.scalar_one_or_none() is not None:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Symbol {req.symbol} already in watchlist",
        )

    item = WatchlistItem(
        user_id=user_id,
        symbol=req.symbol,
        notes=req.notes,
        alert_on_signal=req.alert_on_signal,
    )
    db.add(item)
    await db.flush()
    await db.refresh(item)

    # Update Redis watcher set for signal routing
    if redis is not None:
        try:
            await redis.sadd(f"agent:watchers:{req.symbol}", str(user_id))
            await redis.expire(f"agent:watchers:{req.symbol}", 86400)
        except Exception as exc:
            logger.warning("Redis SADD failed", symbol=req.symbol, error=str(exc))

    return item


async def list_watchlist(user_id: uuid.UUID, db: AsyncSession) -> list[WatchlistItem]:
    result = await db.execute(
        select(WatchlistItem)
        .where(WatchlistItem.user_id == user_id)
        .order_by(WatchlistItem.created_at.asc())
    )
    return result.scalars().all()


async def remove_watchlist_item(
    user_id: uuid.UUID,
    symbol: str,
    db: AsyncSession,
    redis=None,
) -> None:
    result = await db.execute(
        select(WatchlistItem).where(
            WatchlistItem.user_id == user_id,
            WatchlistItem.symbol == symbol.upper(),
        )
    )
    item = result.scalar_one_or_none()
    if item is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Symbol {symbol.upper()} not in watchlist",
        )

    await db.delete(item)
    await db.flush()

    # Remove from Redis watcher set
    if redis is not None:
        try:
            await redis.srem(f"agent:watchers:{symbol.upper()}", str(user_id))
        except Exception as exc:
            logger.warning("Redis SREM failed", symbol=symbol, error=str(exc))


# ─── UserPreference ───────────────────────────────────────────────────────────

async def get_or_create_preferences(
    user_id: uuid.UUID,
    db: AsyncSession,
) -> UserPreference:
    result = await db.execute(
        select(UserPreference).where(UserPreference.user_id == user_id)
    )
    pref = result.scalar_one_or_none()
    if pref is None:
        pref = UserPreference(user_id=user_id)
        db.add(pref)
        await db.flush()
        await db.refresh(pref)
    return pref


async def update_preferences(
    user_id: uuid.UUID,
    req: UserPreferenceUpdate,
    db: AsyncSession,
) -> UserPreference:
    pref = await get_or_create_preferences(user_id, db)

    for field, value in req.model_dump(exclude_none=True).items():
        setattr(pref, field, value)

    await db.flush()
    await db.refresh(pref)
    return pref
