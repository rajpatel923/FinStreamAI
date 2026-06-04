"""User profile and API key management routes."""
from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from typing import Annotated

import structlog
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.database import get_db
from src.core.dependencies import get_current_user
from src.core.security import generate_api_key
from src.models.user import ApiKey, User
from src.schemas.user import ApiKeyCreateRequest, ApiKeyResponse, UpdateProfileRequest, UserResponse

logger = structlog.get_logger(__name__)
router = APIRouter(prefix="/users", tags=["users"])


@router.get("/me", response_model=UserResponse)
async def get_me(user: Annotated[User, Depends(get_current_user)]):
    return user


@router.put("/me", response_model=UserResponse)
async def update_me(
    req: UpdateProfileRequest,
    user: Annotated[User, Depends(get_current_user)],
    db: Annotated[AsyncSession, Depends(get_db)],
):
    if req.full_name is not None:
        user.full_name = req.full_name
    await db.flush()
    return user


@router.post("/me/api-keys", response_model=ApiKeyResponse, status_code=status.HTTP_201_CREATED)
async def create_api_key(
    req: ApiKeyCreateRequest,
    user: Annotated[User, Depends(get_current_user)],
    db: Annotated[AsyncSession, Depends(get_db)],
):
    full_key, key_prefix, key_hash = generate_api_key()
    expires_at = None
    if req.expires_days:
        expires_at = datetime.now(timezone.utc) + timedelta(days=req.expires_days)

    api_key = ApiKey(
        user_id=user.id,
        key_prefix=key_prefix,
        key_hash=key_hash,
        name=req.name,
        expires_at=expires_at,
    )
    db.add(api_key)
    await db.flush()

    resp = ApiKeyResponse.model_validate(api_key)
    resp.full_key = full_key  # only returned once
    return resp


@router.get("/me/api-keys", response_model=list[ApiKeyResponse])
async def list_api_keys(
    user: Annotated[User, Depends(get_current_user)],
    db: Annotated[AsyncSession, Depends(get_db)],
):
    result = await db.execute(
        select(ApiKey).where(ApiKey.user_id == user.id).order_by(ApiKey.created_at.desc())
    )
    return result.scalars().all()


@router.delete("/me/api-keys/{key_id}", status_code=status.HTTP_204_NO_CONTENT)
async def revoke_api_key(
    key_id: uuid.UUID,
    user: Annotated[User, Depends(get_current_user)],
    db: Annotated[AsyncSession, Depends(get_db)],
):
    result = await db.execute(
        select(ApiKey).where(ApiKey.id == key_id, ApiKey.user_id == user.id)
    )
    key = result.scalar_one_or_none()
    if key is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="API key not found")
    key.is_active = False
    await db.flush()
