"""User preferences endpoints."""
from __future__ import annotations

from typing import Annotated

import structlog
from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.database import get_db
from src.core.dependencies import get_current_user
from src.models.user import User
from src.schemas.watchlist import UserPreferenceResponse, UserPreferenceUpdate
from src.services import watchlist_service

logger = structlog.get_logger(__name__)
router = APIRouter(prefix="/users/me", tags=["preferences"])


@router.get("/preferences", response_model=UserPreferenceResponse)
async def get_preferences(
    user: Annotated[User, Depends(get_current_user)],
    db: Annotated[AsyncSession, Depends(get_db)],
):
    return await watchlist_service.get_or_create_preferences(user.id, db)


@router.put("/preferences", response_model=UserPreferenceResponse)
async def update_preferences(
    req: UserPreferenceUpdate,
    user: Annotated[User, Depends(get_current_user)],
    db: Annotated[AsyncSession, Depends(get_db)],
):
    return await watchlist_service.update_preferences(user.id, req, db)
