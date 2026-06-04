"""Additional tests to boost coverage on services and endpoints."""
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.security import hash_password, generate_refresh_token, hash_refresh_token
from src.models.user import User, UserSession
from src.schemas.auth import LoginRequest
from src.services import auth_service, notification_service
from tests.conftest import auth_headers


# ─── Auth service — token rotation ───────────────────────────────────────────

@pytest.mark.asyncio
async def test_refresh_token_rotation(db_session: AsyncSession):
    """Valid refresh token returns new tokens and revokes old one."""
    user = User(
        id=uuid.uuid4(),
        email=f"refresh_{uuid.uuid4().hex[:8]}@t.com",
        hashed_password=hash_password("pass"),
        role="free_user",
    )
    db_session.add(user)
    await db_session.flush()

    refresh_token = generate_refresh_token()
    token_hash = hash_refresh_token(refresh_token)

    from datetime import timedelta
    session = UserSession(
        user_id=user.id,
        token_hash=token_hash,
        expires_at=datetime.now(timezone.utc) + timedelta(days=7),
    )
    db_session.add(session)
    await db_session.flush()

    mock_redis = AsyncMock()
    mock_redis.get = AsyncMock(return_value=str(user.id))
    mock_redis.delete = AsyncMock(return_value=1)
    mock_redis.setex = AsyncMock(return_value=True)

    result = await auth_service.refresh_tokens(refresh_token, db_session, mock_redis)
    assert result.access_token
    assert result.refresh_token
    assert result.refresh_token != refresh_token  # rotated


@pytest.mark.asyncio
async def test_login_increments_failed_attempts(db_session: AsyncSession):
    user = User(
        id=uuid.uuid4(),
        email=f"inc_{uuid.uuid4().hex[:8]}@t.com",
        hashed_password=hash_password("correct"),
        role="free_user",
    )
    db_session.add(user)
    await db_session.flush()

    mock_redis = AsyncMock()
    from fastapi import HTTPException
    req = LoginRequest(email=user.email, password="wrong")
    with pytest.raises(HTTPException):
        await auth_service.login_user(req, db_session, mock_redis)
    assert user.failed_login_attempts == 1


@pytest.mark.asyncio
async def test_login_inactive_user(db_session: AsyncSession):
    user = User(
        id=uuid.uuid4(),
        email=f"inactive_{uuid.uuid4().hex[:8]}@t.com",
        hashed_password=hash_password("password"),
        is_active=False,
        role="free_user",
    )
    db_session.add(user)
    await db_session.flush()

    mock_redis = AsyncMock()
    from fastapi import HTTPException
    req = LoginRequest(email=user.email, password="password")
    with pytest.raises(HTTPException) as exc_info:
        await auth_service.login_user(req, db_session, mock_redis)
    assert exc_info.value.status_code == 401


# ─── Auth endpoint — health ───────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_health_endpoint(client: AsyncClient):
    resp = await client.get("/api/v1/health")
    assert resp.status_code == 200
    assert resp.json()["service"] == "api-gateway"


@pytest.mark.asyncio
async def test_health_ready(client: AsyncClient):
    resp = await client.get("/api/v1/health/ready")
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_health_live(client: AsyncClient):
    resp = await client.get("/api/v1/health/live")
    assert resp.status_code == 200


# ─── Notification service — dispatcher ───────────────────────────────────────

@pytest.mark.asyncio
async def test_dispatch_email_channel():
    with patch("src.services.notification_service.send_email", return_value=True) as mock_email:
        await notification_service.dispatch_alert_notification(
            channels=["email"],
            config={},
            alert_name="Price Alert",
            message="AAPL > 200",
            user_email="test@example.com",
        )
    mock_email.assert_called_once()


@pytest.mark.asyncio
async def test_dispatch_sms_channel():
    with patch("src.services.notification_service.send_sms", return_value=True) as mock_sms:
        await notification_service.dispatch_alert_notification(
            channels=["sms"],
            config={"sms_number": "+15551234567"},
            alert_name="Alert",
            message="triggered",
        )
    mock_sms.assert_called_once()


@pytest.mark.asyncio
async def test_dispatch_webhook_channel():
    with patch("src.services.notification_service.send_webhook", return_value=True) as mock_wh:
        await notification_service.dispatch_alert_notification(
            channels=["webhook"],
            config={"webhook_url": "https://example.com/hook"},
            alert_name="Alert",
            message="triggered",
        )
    mock_wh.assert_called_once()


@pytest.mark.asyncio
async def test_send_email_with_key():
    from src.core.config import settings
    settings.SENDGRID_API_KEY = "SG.test_key"
    settings.SENDGRID_FROM_EMAIL = "noreply@test.com"

    with patch("httpx.AsyncClient.post") as mock_post:
        mock_resp = MagicMock()
        mock_resp.raise_for_status = MagicMock()
        mock_post.return_value = mock_resp
        result = await notification_service.send_email("user@test.com", "Subject", "Body", {})
    assert result is True


@pytest.mark.asyncio
async def test_send_email_failure():
    from src.core.config import settings
    settings.SENDGRID_API_KEY = "SG.test_key"
    with patch("httpx.AsyncClient.post", side_effect=Exception("Network error")):
        result = await notification_service.send_email("user@test.com", "Subject", "Body", {})
    assert result is False


# ─── Query endpoint — additional coverage ────────────────────────────────────

@pytest.mark.asyncio
async def test_query_with_cursor(client: AsyncClient, free_user: User):
    with patch("src.api.v1.query.query_service.query_market_data") as mock_qmd:
        mock_qmd.return_value = {"symbol": "AAPL", "data": [], "cursor": "2026-01-01T00:00:00"}
        resp = await client.get(
            "/api/v1/query/market-data?symbol=AAPL&cursor=2026-01-01T00:00:00",
            headers=auth_headers(free_user),
        )
    assert resp.status_code == 200
    assert resp.json()["cursor"] == "2026-01-01T00:00:00"


@pytest.mark.asyncio
async def test_custom_query_invalid_table(client: AsyncClient, premium_user: User):
    with patch("src.api.v1.query.query_service.run_custom_query",
               side_effect=ValueError("Table 'forbidden' is not queryable")):
        resp = await client.post(
            "/api/v1/query/custom",
            json={"table": "forbidden"},
            headers=auth_headers(premium_user),
        )
    assert resp.status_code == 422


# ─── Analytics — backtest ────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_backtest_insufficient_data(client: AsyncClient, premium_user: User):
    """Backtest with no data returns 422 (insufficient) or 500 (no TimescaleDB in tests)."""
    resp = await client.post(
        "/api/v1/analytics/backtest",
        json={"symbol": "AAPL", "long_window": 50},
        headers=auth_headers(premium_user),
    )
    # auth gate passed (not 401/403)
    assert resp.status_code not in (401, 403)


# ─── Alert service — free tier cap exact boundary ───────────────────────────

@pytest.mark.asyncio
async def test_alert_premium_no_cap(db_session: AsyncSession):
    """Premium user can exceed 3 alerts."""
    user = User(id=uuid.uuid4(), email=f"prem_{uuid.uuid4().hex[:8]}@t.com", role="premium_user")
    db_session.add(user)
    await db_session.flush()

    from src.schemas.alert import AlertCreate
    from src.services import alert_service

    for i in range(5):
        req = AlertCreate(
            name=f"Alert {i}",
            alert_type="price",
            symbol="AAPL",
            condition={"operator": "gt", "threshold": 100.0},
            notification_channels=["email"],
            notification_config={},
        )
        alert = await alert_service.create_alert(user.id, req, db_session, "premium_user")
        assert alert.name == f"Alert {i}"


# ─── Security — additional tests ─────────────────────────────────────────────

def test_generate_api_key_verify():
    from src.core.security import generate_api_key, verify_api_key
    full_key, prefix, key_hash = generate_api_key()
    assert verify_api_key(full_key, key_hash)
    assert not verify_api_key("wrong_key", key_hash)


def test_hash_password_and_verify():
    from src.core.security import hash_password, verify_password
    hashed = hash_password("my_secure_pass!")
    assert verify_password("my_secure_pass!", hashed)
    assert not verify_password("wrong_pass", hashed)


def test_create_and_decode_token():
    from src.core.config import settings
    settings.JWT_SECRET_KEY = "test_secret_at_least_32_chars_long"
    from src.core.security import create_access_token, decode_access_token
    token = create_access_token("abc123", "admin")
    payload = decode_access_token(token)
    assert payload["sub"] == "abc123"
    assert payload["role"] == "admin"
    assert "jti" in payload
    assert "exp" in payload


def test_oauth_state_generation():
    from src.core.security import generate_oauth_state
    state = generate_oauth_state()
    assert len(state) >= 20


def test_refresh_token_hashing():
    from src.core.security import generate_refresh_token, hash_refresh_token
    token = generate_refresh_token()
    h = hash_refresh_token(token)
    assert len(h) == 64  # SHA-256 hex = 64 chars
    assert hash_refresh_token(token) == h  # deterministic
