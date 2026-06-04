"""OAuth 2.0 service: Google + GitHub authorization flow."""
from __future__ import annotations

import structlog
import httpx
import redis.asyncio as aioredis
from fastapi import HTTPException, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.config import settings
from src.core.security import generate_oauth_state
from src.models.user import User
from src.services.auth_service import _issue_tokens
from src.schemas.auth import TokenResponse

logger = structlog.get_logger(__name__)

_GOOGLE_AUTH_URL = "https://accounts.google.com/o/oauth2/v2/auth"
_GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"
_GOOGLE_USERINFO_URL = "https://www.googleapis.com/oauth2/v3/userinfo"

_GITHUB_AUTH_URL = "https://github.com/login/oauth/authorize"
_GITHUB_TOKEN_URL = "https://github.com/login/oauth/access_token"
_GITHUB_USER_URL = "https://api.github.com/user"
_GITHUB_EMAIL_URL = "https://api.github.com/user/emails"


async def get_google_authorization_url(redis: aioredis.Redis) -> str:
    state = generate_oauth_state()
    await redis.setex(f"oauth:state:{state}", 600, "google")
    redirect_uri = f"{settings.OAUTH_REDIRECT_BASE_URL}/api/v1/auth/oauth/google/callback"
    params = (
        f"client_id={settings.GOOGLE_CLIENT_ID}"
        f"&redirect_uri={redirect_uri}"
        f"&response_type=code"
        f"&scope=openid+email+profile"
        f"&state={state}"
    )
    return f"{_GOOGLE_AUTH_URL}?{params}"


async def handle_google_callback(
    code: str,
    state: str,
    db: AsyncSession,
    redis: aioredis.Redis,
) -> TokenResponse:
    await _validate_state(state, redis)
    redirect_uri = f"{settings.OAUTH_REDIRECT_BASE_URL}/api/v1/auth/oauth/google/callback"
    async with httpx.AsyncClient(timeout=10) as client:
        token_resp = await client.post(
            _GOOGLE_TOKEN_URL,
            data={
                "code": code,
                "client_id": settings.GOOGLE_CLIENT_ID,
                "client_secret": settings.GOOGLE_CLIENT_SECRET,
                "redirect_uri": redirect_uri,
                "grant_type": "authorization_code",
            },
        )
        token_resp.raise_for_status()
        access_token = token_resp.json()["access_token"]

        user_resp = await client.get(
            _GOOGLE_USERINFO_URL,
            headers={"Authorization": f"Bearer {access_token}"},
        )
        user_resp.raise_for_status()
        info = user_resp.json()

    email = info.get("email")
    subject = info.get("sub")
    full_name = info.get("name")
    if not email or not subject:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Google did not return email")

    user = await _upsert_oauth_user(db, email, "google", subject, full_name)
    return await _issue_tokens(user, db, redis, None, None)


async def get_github_authorization_url(redis: aioredis.Redis) -> str:
    state = generate_oauth_state()
    await redis.setex(f"oauth:state:{state}", 600, "github")
    redirect_uri = f"{settings.OAUTH_REDIRECT_BASE_URL}/api/v1/auth/oauth/github/callback"
    params = (
        f"client_id={settings.GITHUB_CLIENT_ID}"
        f"&redirect_uri={redirect_uri}"
        f"&scope=read:user+user:email"
        f"&state={state}"
    )
    return f"{_GITHUB_AUTH_URL}?{params}"


async def handle_github_callback(
    code: str,
    state: str,
    db: AsyncSession,
    redis: aioredis.Redis,
) -> TokenResponse:
    await _validate_state(state, redis)
    redirect_uri = f"{settings.OAUTH_REDIRECT_BASE_URL}/api/v1/auth/oauth/github/callback"
    async with httpx.AsyncClient(timeout=10) as client:
        token_resp = await client.post(
            _GITHUB_TOKEN_URL,
            data={
                "client_id": settings.GITHUB_CLIENT_ID,
                "client_secret": settings.GITHUB_CLIENT_SECRET,
                "code": code,
                "redirect_uri": redirect_uri,
            },
            headers={"Accept": "application/json"},
        )
        token_resp.raise_for_status()
        access_token = token_resp.json()["access_token"]
        headers = {"Authorization": f"token {access_token}", "Accept": "application/json"}

        user_resp = await client.get(_GITHUB_USER_URL, headers=headers)
        user_resp.raise_for_status()
        user_data = user_resp.json()
        subject = str(user_data["id"])
        full_name = user_data.get("name")

        email = user_data.get("email")
        if not email:
            email_resp = await client.get(_GITHUB_EMAIL_URL, headers=headers)
            email_resp.raise_for_status()
            primary = next(
                (e for e in email_resp.json() if e.get("primary") and e.get("verified")), None
            )
            if primary:
                email = primary["email"]

    if not email:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="GitHub did not return a verified email")

    user = await _upsert_oauth_user(db, email, "github", subject, full_name)
    return await _issue_tokens(user, db, redis, None, None)


async def _validate_state(state: str, redis: aioredis.Redis) -> None:
    key = f"oauth:state:{state}"
    stored = await redis.get(key)
    if not stored:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid or expired OAuth state")
    await redis.delete(key)


async def _upsert_oauth_user(
    db: AsyncSession,
    email: str,
    provider: str,
    subject: str,
    full_name: str | None,
) -> User:
    result = await db.execute(select(User).where(User.email == email))
    user = result.scalar_one_or_none()
    if user:
        # Update OAuth info if logging in via OAuth for first time
        user.oauth_provider = provider
        user.oauth_subject = subject
        if full_name and not user.full_name:
            user.full_name = full_name
    else:
        user = User(
            email=email,
            oauth_provider=provider,
            oauth_subject=subject,
            full_name=full_name,
            role="free_user",
        )
        db.add(user)
    await db.flush()
    return user
