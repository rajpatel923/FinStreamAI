"""Tests for OAuth service: state validation, user upsert."""
from __future__ import annotations

import uuid
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.security import generate_oauth_state
from src.models.user import User
from src.services import oauth_service


@pytest.mark.asyncio
async def test_validate_state_valid(mock_redis):
    mock_redis.get = AsyncMock(return_value="google")
    mock_redis.delete = AsyncMock(return_value=1)
    # Should not raise
    await oauth_service._validate_state("valid_state", mock_redis)


@pytest.mark.asyncio
async def test_validate_state_invalid(mock_redis):
    mock_redis.get = AsyncMock(return_value=None)
    from fastapi import HTTPException
    with pytest.raises(HTTPException) as exc_info:
        await oauth_service._validate_state("bad_state", mock_redis)
    assert exc_info.value.status_code == 400


@pytest.mark.asyncio
async def test_upsert_oauth_user_new(db_session: AsyncSession):
    email = f"oauth_{uuid.uuid4().hex[:8]}@test.com"
    user = await oauth_service._upsert_oauth_user(db_session, email, "google", "sub123", "Test User")
    assert user.email == email
    assert user.oauth_provider == "google"
    assert user.role == "free_user"


@pytest.mark.asyncio
async def test_upsert_oauth_user_existing(db_session: AsyncSession):
    email = f"existing_{uuid.uuid4().hex[:8]}@test.com"
    existing = User(id=uuid.uuid4(), email=email, role="premium_user")
    db_session.add(existing)
    await db_session.flush()

    user = await oauth_service._upsert_oauth_user(db_session, email, "github", "ghsub", "GH Name")
    assert user.role == "premium_user"  # role preserved
    assert user.oauth_provider == "github"


@pytest.mark.asyncio
async def test_get_google_authorization_url(mock_redis):
    from src.core.config import settings
    settings.GOOGLE_CLIENT_ID = "test_client"
    url = await oauth_service.get_google_authorization_url(mock_redis)
    assert "accounts.google.com" in url
    assert "test_client" in url
    assert "state=" in url


@pytest.mark.asyncio
async def test_get_github_authorization_url(mock_redis):
    from src.core.config import settings
    settings.GITHUB_CLIENT_ID = "gh_test_client"
    url = await oauth_service.get_github_authorization_url(mock_redis)
    assert "github.com/login/oauth/authorize" in url
    assert "gh_test_client" in url
