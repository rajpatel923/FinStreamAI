"""Unit tests for service layer: auth, alert, query, notification."""
from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.security import hash_password
from src.models.alert import Alert
from src.models.user import User
from src.schemas.alert import AlertCreate, AlertUpdate
from src.schemas.auth import LoginRequest, RegisterRequest
from src.services import alert_service, auth_service, notification_service


# ─── Auth service ────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_register_user(db_session: AsyncSession):
    req = RegisterRequest(email=f"reg_{uuid.uuid4().hex[:8]}@test.com", password="Password1!")
    user = await auth_service.register_user(req, db_session)
    assert user.email == req.email
    assert user.role == "free_user"


@pytest.mark.asyncio
async def test_register_duplicate_raises(db_session: AsyncSession):
    email = f"dup_{uuid.uuid4().hex[:8]}@test.com"
    req = RegisterRequest(email=email, password="Password1!")
    await auth_service.register_user(req, db_session)
    from fastapi import HTTPException
    with pytest.raises(HTTPException) as exc_info:
        await auth_service.register_user(req, db_session)
    assert exc_info.value.status_code == 400


@pytest.mark.asyncio
async def test_login_success(db_session: AsyncSession):
    user = User(
        id=uuid.uuid4(),
        email=f"login_{uuid.uuid4().hex[:8]}@test.com",
        hashed_password=hash_password("correctpass"),
        role="free_user",
    )
    db_session.add(user)
    await db_session.flush()

    mock_redis = AsyncMock()
    mock_redis.setex = AsyncMock(return_value=True)

    req = LoginRequest(email=user.email, password="correctpass")
    token = await auth_service.login_user(req, db_session, mock_redis)
    assert token.access_token
    assert token.refresh_token


@pytest.mark.asyncio
async def test_login_bad_password(db_session: AsyncSession):
    user = User(
        id=uuid.uuid4(),
        email=f"bad_{uuid.uuid4().hex[:8]}@test.com",
        hashed_password=hash_password("correctpass"),
        role="free_user",
    )
    db_session.add(user)
    await db_session.flush()

    mock_redis = AsyncMock()
    from fastapi import HTTPException
    req = LoginRequest(email=user.email, password="wrongpass")
    with pytest.raises(HTTPException) as exc_info:
        await auth_service.login_user(req, db_session, mock_redis)
    assert exc_info.value.status_code == 401


@pytest.mark.asyncio
async def test_login_locked_account(db_session: AsyncSession):
    user = User(
        id=uuid.uuid4(),
        email=f"locked_{uuid.uuid4().hex[:8]}@test.com",
        hashed_password=hash_password("correctpass"),
        role="free_user",
        locked_until=datetime.now(timezone.utc) + timedelta(minutes=10),
    )
    db_session.add(user)
    await db_session.flush()

    mock_redis = AsyncMock()
    from fastapi import HTTPException
    req = LoginRequest(email=user.email, password="correctpass")
    with pytest.raises(HTTPException) as exc_info:
        await auth_service.login_user(req, db_session, mock_redis)
    assert exc_info.value.status_code == 423


@pytest.mark.asyncio
async def test_refresh_invalid_token(db_session: AsyncSession):
    mock_redis = AsyncMock()
    mock_redis.get = AsyncMock(return_value=None)
    from fastapi import HTTPException
    with pytest.raises(HTTPException) as exc_info:
        await auth_service.refresh_tokens("bogus_token", db_session, mock_redis)
    assert exc_info.value.status_code == 401


@pytest.mark.asyncio
async def test_logout(db_session: AsyncSession):
    mock_redis = AsyncMock()
    mock_redis.delete = AsyncMock(return_value=1)
    await auth_service.logout_user("some_token", db_session, mock_redis)
    mock_redis.delete.assert_called_once()


# ─── Alert service ───────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_alert_crud(db_session: AsyncSession):
    user = User(id=uuid.uuid4(), email=f"a_{uuid.uuid4().hex[:8]}@t.com", role="free_user")
    db_session.add(user)
    await db_session.flush()

    req = AlertCreate(
        name="Test Alert",
        alert_type="price",
        symbol="AAPL",
        condition={"operator": "gt", "threshold": 150.0},
        notification_channels=["email"],
        notification_config={},
    )
    alert = await alert_service.create_alert(user.id, req, db_session, "free_user")
    assert alert.name == "Test Alert"

    alerts = await alert_service.list_alerts(user.id, db_session)
    assert len(alerts) == 1

    update_req = AlertUpdate(name="Updated Alert", is_active=False)
    updated = await alert_service.update_alert(alert.id, user.id, update_req, db_session)
    assert updated.name == "Updated Alert"
    assert not updated.is_active

    await alert_service.delete_alert(alert.id, user.id, db_session)
    alerts_after = await alert_service.list_alerts(user.id, db_session)
    assert len(alerts_after) == 0


@pytest.mark.asyncio
async def test_alert_not_found(db_session: AsyncSession):
    user = User(id=uuid.uuid4(), email=f"b_{uuid.uuid4().hex[:8]}@t.com", role="free_user")
    db_session.add(user)
    await db_session.flush()
    result = await alert_service.get_alert(uuid.uuid4(), user.id, db_session)
    assert result is None


def test_evaluate_condition():
    from src.services.alert_service import evaluate_condition
    assert evaluate_condition({"operator": "gt", "threshold": 100.0}, 150.0)
    assert not evaluate_condition({"operator": "gt", "threshold": 100.0}, 50.0)
    assert evaluate_condition({"operator": "lt", "threshold": 100.0}, 50.0)
    assert evaluate_condition({"operator": "gte", "threshold": 100.0}, 100.0)
    assert evaluate_condition({"operator": "lte", "threshold": 100.0}, 100.0)
    assert evaluate_condition({"operator": "eq", "threshold": 100.0}, 100.0)
    assert not evaluate_condition({"operator": "unknown"}, 100.0)


# ─── Notification service ────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_send_email_no_key():
    from src.core.config import settings
    settings.SENDGRID_API_KEY = ""
    result = await notification_service.send_email("test@test.com", "Hi", "Body", {})
    assert result is False


@pytest.mark.asyncio
async def test_send_sms_no_config():
    from src.core.config import settings
    settings.TWILIO_ACCOUNT_SID = ""
    result = await notification_service.send_sms("+1234567890", "Hello")
    assert result is False


@pytest.mark.asyncio
async def test_send_slack_success():
    with patch("httpx.AsyncClient.post") as mock_post:
        mock_resp = MagicMock()
        mock_resp.raise_for_status = MagicMock()
        mock_post.return_value = mock_resp
        result = await notification_service.send_slack("https://hooks.slack.com/test", "Alert!")
    assert result is True


@pytest.mark.asyncio
async def test_send_slack_failure():
    with patch("httpx.AsyncClient.post", side_effect=Exception("connection refused")):
        result = await notification_service.send_slack("https://hooks.slack.com/test", "Alert!")
    assert result is False


@pytest.mark.asyncio
async def test_send_webhook_success():
    with patch("httpx.AsyncClient.post") as mock_post:
        mock_resp = MagicMock()
        mock_resp.raise_for_status = MagicMock()
        mock_post.return_value = mock_resp
        result = await notification_service.send_webhook(
            "https://example.com/hook", {"event": "alert"}
        )
    assert result is True


@pytest.mark.asyncio
async def test_dispatch_alert_notification():
    with patch("src.services.notification_service.send_email", return_value=True) as mock_email:
        with patch("src.services.notification_service.send_slack", return_value=True) as mock_slack:
            await notification_service.dispatch_alert_notification(
                channels=["email", "slack"],
                config={"slack_url": "https://hooks.slack.com/x"},
                alert_name="Test",
                message="Price exceeded threshold",
                user_email="user@test.com",
            )
    mock_email.assert_called_once()
    mock_slack.assert_called_once()


# ─── Query service helpers ────────────────────────────────────────────────────

def test_validate_webhook_url_variants():
    from src.core.security import validate_webhook_url
    # Test IPv6 loopback
    assert not validate_webhook_url("http://[::1]/internal")
    # Test a normal public URL
    assert validate_webhook_url("https://api.example.com/webhook")


# ─── Dependencies ────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_require_role_passes(db_session: AsyncSession):
    user = User(id=uuid.uuid4(), email=f"r_{uuid.uuid4().hex[:8]}@t.com", role="admin")
    db_session.add(user)
    await db_session.flush()

    from src.core.dependencies import require_role
    checker = require_role("admin", "premium_user")
    result = await checker(user)
    assert result == user


@pytest.mark.asyncio
async def test_require_role_fails():
    user = User(id=uuid.uuid4(), email=f"f_{uuid.uuid4().hex[:8]}@t.com", role="free_user")

    from fastapi import HTTPException
    from src.core.dependencies import require_role
    checker = require_role("admin", "premium_user")
    with pytest.raises(HTTPException) as exc_info:
        await checker(user)
    assert exc_info.value.status_code == 403
