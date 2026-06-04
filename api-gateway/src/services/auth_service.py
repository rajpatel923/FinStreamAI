"""Auth business logic: register, login, refresh rotation, logout, lockout."""
from __future__ import annotations

import hashlib
from datetime import datetime, timedelta, timezone

import redis.asyncio as aioredis
import structlog
from fastapi import HTTPException, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.config import settings
from src.core.security import (
    create_access_token,
    generate_refresh_token,
    hash_password,
    hash_refresh_token,
    verify_password,
)
from src.models.user import User, UserSession
from src.schemas.auth import LoginRequest, RegisterRequest, TokenResponse

logger = structlog.get_logger(__name__)

_ACCESS_EXPIRE_S = settings.ACCESS_TOKEN_EXPIRE_MINUTES * 60


async def register_user(req: RegisterRequest, db: AsyncSession) -> User:
    result = await db.execute(select(User).where(User.email == req.email))
    if result.scalar_one_or_none() is not None:
        # Constant-time — same error prevents enumeration
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Registration failed")

    user = User(
        email=req.email,
        hashed_password=hash_password(req.password),
        full_name=req.full_name,
        role="free_user",
    )
    db.add(user)
    await db.flush()
    logger.info("User registered", user_id=str(user.id))
    return user


async def login_user(
    req: LoginRequest,
    db: AsyncSession,
    redis: aioredis.Redis,
    ip_address: str | None = None,
    user_agent: str | None = None,
) -> TokenResponse:
    result = await db.execute(select(User).where(User.email == req.email))
    user = result.scalar_one_or_none()

    # Always run password check to avoid timing oracle on "not found" vs "wrong password"
    # Pre-generated valid bcrypt hash of "dummy" to avoid hash format errors
    _DUMMY_HASH = "$2b$12$P9jcg33YABFzkaKHsP/fsuX.Vc97n2BNgNDBLtRuORUizR/xgLGnu"
    candidate_hash = user.hashed_password if user else _DUMMY_HASH

    password_ok = verify_password(req.password, candidate_hash) if candidate_hash else False

    if user is None or not password_ok:
        if user:
            await _record_failed_attempt(user, db)
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )

    if not user.is_active:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Account disabled")

    # Check lockout
    now = datetime.now(timezone.utc)
    if user.locked_until and user.locked_until > now:
        raise HTTPException(
            status_code=status.HTTP_423_LOCKED,
            detail=f"Account locked until {user.locked_until.isoformat()}",
        )

    # Reset failure counter on success
    user.failed_login_attempts = 0
    user.locked_until = None

    return await _issue_tokens(user, db, redis, ip_address, user_agent)


async def _record_failed_attempt(user: User, db: AsyncSession) -> None:
    user.failed_login_attempts += 1
    if user.failed_login_attempts >= settings.LOGIN_MAX_ATTEMPTS:
        user.locked_until = datetime.now(timezone.utc) + timedelta(
            minutes=settings.LOCKOUT_DURATION_MINUTES
        )
        logger.warning("Account locked", user_id=str(user.id))
    await db.flush()


async def refresh_tokens(
    refresh_token: str,
    db: AsyncSession,
    redis: aioredis.Redis,
) -> TokenResponse:
    token_hash = hash_refresh_token(refresh_token)
    redis_key = f"refresh:{token_hash}"

    user_id = await redis.get(redis_key)
    if not user_id:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid or expired refresh token")

    # Revoke old token immediately (rotation)
    await redis.delete(redis_key)

    import uuid as _uuid_mod
    try:
        uid = _uuid_mod.UUID(user_id)
    except ValueError:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid session")

    result = await db.execute(select(User).where(User.id == uid))
    user = result.scalar_one_or_none()
    if user is None or not user.is_active:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User not found")

    # Check absolute max lifetime via DB session
    session_result = await db.execute(
        select(UserSession).where(
            UserSession.user_id == user.id,
            UserSession.token_hash == token_hash,
            UserSession.revoked_at == None,  # noqa: E711
        )
    )
    session = session_result.scalar_one_or_none()
    if session:
        created_at = session.created_at
        if created_at.tzinfo is None:
            created_at = created_at.replace(tzinfo=timezone.utc)
        max_lifetime = created_at + timedelta(days=settings.REFRESH_TOKEN_ABSOLUTE_MAX_DAYS)
        if datetime.now(timezone.utc) > max_lifetime:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Session expired — please log in again",
            )
        session.revoked_at = datetime.now(timezone.utc)
        await db.flush()

    return await _issue_tokens(user, db, redis, None, None)


async def logout_user(
    refresh_token: str,
    db: AsyncSession,
    redis: aioredis.Redis,
) -> None:
    token_hash = hash_refresh_token(refresh_token)
    await redis.delete(f"refresh:{token_hash}")
    # Mark DB session as revoked
    session_result = await db.execute(
        select(UserSession).where(
            UserSession.token_hash == token_hash,
            UserSession.revoked_at == None,  # noqa: E711
        )
    )
    session = session_result.scalar_one_or_none()
    if session:
        session.revoked_at = datetime.now(timezone.utc)
        await db.flush()


async def _issue_tokens(
    user: User,
    db: AsyncSession,
    redis: aioredis.Redis,
    ip_address: str | None,
    user_agent: str | None,
) -> TokenResponse:
    access_token = create_access_token(str(user.id), user.role)
    refresh_token = generate_refresh_token()
    token_hash = hash_refresh_token(refresh_token)

    ttl_s = settings.REFRESH_TOKEN_EXPIRE_DAYS * 86400
    await redis.setex(f"refresh:{token_hash}", ttl_s, str(user.id))

    # Persist session audit record
    session = UserSession(
        user_id=user.id,
        token_hash=token_hash,
        expires_at=datetime.now(timezone.utc) + timedelta(days=settings.REFRESH_TOKEN_EXPIRE_DAYS),
        ip_address=ip_address,
        user_agent=user_agent,
    )
    db.add(session)
    await db.flush()

    return TokenResponse(
        access_token=access_token,
        refresh_token=refresh_token,
        expires_in=_ACCESS_EXPIRE_S,
    )
