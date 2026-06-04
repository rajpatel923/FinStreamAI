"""Auth routes: register, login, refresh, logout, OAuth."""
from __future__ import annotations

from typing import Annotated

import redis.asyncio as aioredis
import structlog
from fastapi import APIRouter, Depends, Request, status
from fastapi.responses import RedirectResponse
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.dependencies import get_current_user, get_redis
from src.core.database import get_db
from src.models.user import User
from src.schemas.auth import LoginRequest, LogoutRequest, RefreshRequest, RegisterRequest, TokenResponse
from src.schemas.user import UserResponse
from src.services import auth_service, oauth_service

logger = structlog.get_logger(__name__)
router = APIRouter(prefix="/auth", tags=["auth"])


@router.post("/register", response_model=UserResponse, status_code=status.HTTP_201_CREATED)
async def register(
    req: RegisterRequest,
    db: Annotated[AsyncSession, Depends(get_db)],
):
    user = await auth_service.register_user(req, db)
    return user


@router.post("/login", response_model=TokenResponse)
async def login(
    req: LoginRequest,
    request: Request,
    db: Annotated[AsyncSession, Depends(get_db)],
    redis: Annotated[aioredis.Redis, Depends(get_redis)],
):
    ip = request.client.host if request.client else None
    ua = request.headers.get("User-Agent")
    return await auth_service.login_user(req, db, redis, ip_address=ip, user_agent=ua)


@router.post("/refresh", response_model=TokenResponse)
async def refresh(
    req: RefreshRequest,
    db: Annotated[AsyncSession, Depends(get_db)],
    redis: Annotated[aioredis.Redis, Depends(get_redis)],
):
    return await auth_service.refresh_tokens(req.refresh_token, db, redis)


@router.post("/logout", status_code=status.HTTP_204_NO_CONTENT)
async def logout(
    req: LogoutRequest,
    db: Annotated[AsyncSession, Depends(get_db)],
    redis: Annotated[aioredis.Redis, Depends(get_redis)],
    _: Annotated[User, Depends(get_current_user)],
):
    await auth_service.logout_user(req.refresh_token, db, redis)


# ─── OAuth ────────────────────────────────────────────────────────────────────

@router.get("/oauth/google")
async def oauth_google_start(
    redis: Annotated[aioredis.Redis, Depends(get_redis)],
):
    url = await oauth_service.get_google_authorization_url(redis)
    return RedirectResponse(url)


@router.get("/oauth/google/callback", response_model=TokenResponse)
async def oauth_google_callback(
    code: str,
    state: str,
    db: Annotated[AsyncSession, Depends(get_db)],
    redis: Annotated[aioredis.Redis, Depends(get_redis)],
):
    return await oauth_service.handle_google_callback(code, state, db, redis)


@router.get("/oauth/github")
async def oauth_github_start(
    redis: Annotated[aioredis.Redis, Depends(get_redis)],
):
    url = await oauth_service.get_github_authorization_url(redis)
    return RedirectResponse(url)


@router.get("/oauth/github/callback", response_model=TokenResponse)
async def oauth_github_callback(
    code: str,
    state: str,
    db: Annotated[AsyncSession, Depends(get_db)],
    redis: Annotated[aioredis.Redis, Depends(get_redis)],
):
    return await oauth_service.handle_github_callback(code, state, db, redis)
