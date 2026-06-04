"""FastAPI dependencies: auth, role enforcement, Redis."""
from __future__ import annotations

import hashlib
import uuid as _uuid_mod
from typing import Annotated

import redis.asyncio as aioredis
import structlog
from fastapi import Depends, HTTPException, Request, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jose import JWTError
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from .config import settings
from .database import get_db
from .security import decode_access_token, verify_api_key
from src.models.user import ApiKey, User

logger = structlog.get_logger(__name__)

_bearer = HTTPBearer(auto_error=False)

# ─── Redis singleton ─────────────────────────────────────────────────────────

_redis_client: aioredis.Redis | None = None


def get_redis() -> aioredis.Redis:
    global _redis_client
    if _redis_client is None:
        _redis_client = aioredis.from_url(settings.redis_url, decode_responses=True)
    return _redis_client


# ─── Auth helpers ────────────────────────────────────────────────────────────

async def _user_from_jwt(token: str, db: AsyncSession) -> User:
    try:
        payload = decode_access_token(token)
    except JWTError as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired token",
            headers={"WWW-Authenticate": "Bearer"},
        ) from e

    user_id = payload.get("sub")
    if not user_id:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token payload")

    try:
        uid = _uuid_mod.UUID(user_id)
    except ValueError:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token payload")

    result = await db.execute(select(User).where(User.id == uid))
    user = result.scalar_one_or_none()
    if user is None or not user.is_active:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User not found or inactive")
    return user


async def _user_from_api_key(raw_key: str, db: AsyncSession, redis: aioredis.Redis) -> User:
    prefix = raw_key[:10]
    cache_key = f"apikey:cache:{prefix}"
    cached_user_id = await redis.get(cache_key)

    if cached_user_id:
        try:
            cached_uid = _uuid_mod.UUID(cached_user_id)
        except ValueError:
            cached_uid = cached_user_id
        result = await db.execute(select(User).where(User.id == cached_uid))
        user = result.scalar_one_or_none()
        if user and user.is_active:
            return user

    # Cache miss — bcrypt check
    result = await db.execute(
        select(ApiKey).where(ApiKey.key_prefix == prefix, ApiKey.is_active == True)  # noqa: E712
    )
    keys = result.scalars().all()
    matched_key: ApiKey | None = None
    for k in keys:
        if verify_api_key(raw_key, k.key_hash):
            matched_key = k
            break

    if matched_key is None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid API key")

    result = await db.execute(select(User).where(User.id == matched_key.user_id))
    user = result.scalar_one_or_none()
    if user is None or not user.is_active:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User not found or inactive")

    # Cache for 5 minutes
    await redis.setex(cache_key, 300, str(user.id))
    return user


# ─── Main dependency ─────────────────────────────────────────────────────────

async def get_current_user(
    request: Request,
    credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(_bearer)],
    db: Annotated[AsyncSession, Depends(get_db)],
) -> User:
    redis = get_redis()

    if credentials:
        scheme = credentials.scheme.lower()
        token = credentials.credentials
        if scheme == "bearer":
            return await _user_from_jwt(token, db)
        if scheme == "apikey":
            return await _user_from_api_key(token, db, redis)

    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Not authenticated",
        headers={"WWW-Authenticate": "Bearer"},
    )


async def optional_user(
    request: Request,
    credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(_bearer)],
    db: Annotated[AsyncSession, Depends(get_db)],
) -> User | None:
    if credentials is None:
        return None
    try:
        return await get_current_user(request, credentials, db)
    except HTTPException:
        return None


def require_role(*roles: str):
    """Dependency factory: enforce that the authenticated user has one of the given roles."""

    async def _check(user: Annotated[User, Depends(get_current_user)]) -> User:
        if user.role not in roles:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Requires role: {' or '.join(roles)}",
            )
        return user

    return _check
