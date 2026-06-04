"""Final coverage boost: oauth callbacks, ws_manager dispatch, analytics endpoints."""
from __future__ import annotations

import asyncio
import uuid
from datetime import datetime, timezone, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from httpx import AsyncClient

from src.models.user import User
from tests.conftest import auth_headers


# ─── OAuth: Google callback ───────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_google_callback_success(db_session):
    from src.services.oauth_service import handle_google_callback

    email = f"google_{uuid.uuid4().hex[:8]}@test.com"

    mock_redis = AsyncMock()
    mock_redis.get = AsyncMock(return_value="google")   # state valid
    mock_redis.delete = AsyncMock()
    mock_redis.setex = AsyncMock()

    token_resp = MagicMock()
    token_resp.raise_for_status = MagicMock()
    token_resp.json = MagicMock(return_value={"access_token": "goog_access_token"})

    user_resp = MagicMock()
    user_resp.raise_for_status = MagicMock()
    user_resp.json = MagicMock(return_value={"email": email, "sub": "goog_sub_123", "name": "Google User"})

    mock_client = AsyncMock()
    mock_client.post = AsyncMock(return_value=token_resp)
    mock_client.get = AsyncMock(return_value=user_resp)
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)

    with patch("httpx.AsyncClient", return_value=mock_client):
        result = await handle_google_callback("auth_code", "valid_state", db_session, mock_redis)

    assert result.access_token


@pytest.mark.asyncio
async def test_google_callback_missing_email(db_session):
    from fastapi import HTTPException
    from src.services.oauth_service import handle_google_callback

    mock_redis = AsyncMock()
    mock_redis.get = AsyncMock(return_value="google")
    mock_redis.delete = AsyncMock()

    token_resp = MagicMock()
    token_resp.raise_for_status = MagicMock()
    token_resp.json = MagicMock(return_value={"access_token": "tok"})

    user_resp = MagicMock()
    user_resp.raise_for_status = MagicMock()
    user_resp.json = MagicMock(return_value={"sub": "sub123"})  # no email

    mock_client = AsyncMock()
    mock_client.post = AsyncMock(return_value=token_resp)
    mock_client.get = AsyncMock(return_value=user_resp)
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)

    with patch("httpx.AsyncClient", return_value=mock_client):
        with pytest.raises(HTTPException) as exc_info:
            await handle_google_callback("code", "state", db_session, mock_redis)
    assert exc_info.value.status_code == 400


# ─── OAuth: GitHub callback ───────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_github_callback_success(db_session):
    from src.services.oauth_service import handle_github_callback

    email = f"gh_{uuid.uuid4().hex[:8]}@test.com"

    mock_redis = AsyncMock()
    mock_redis.get = AsyncMock(return_value="github")
    mock_redis.delete = AsyncMock()
    mock_redis.setex = AsyncMock()

    token_resp = MagicMock()
    token_resp.raise_for_status = MagicMock()
    token_resp.json = MagicMock(return_value={"access_token": "gh_access_token"})

    user_resp = MagicMock()
    user_resp.raise_for_status = MagicMock()
    user_resp.json = MagicMock(return_value={"id": 9876, "name": "GH User", "email": email})

    mock_client = AsyncMock()
    mock_client.post = AsyncMock(return_value=token_resp)
    mock_client.get = AsyncMock(return_value=user_resp)
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)

    with patch("httpx.AsyncClient", return_value=mock_client):
        result = await handle_github_callback("code", "state", db_session, mock_redis)

    assert result.access_token


@pytest.mark.asyncio
async def test_github_callback_email_from_api(db_session):
    """GitHub user has no public email — fallback to emails API."""
    from src.services.oauth_service import handle_github_callback

    email = f"gh2_{uuid.uuid4().hex[:8]}@test.com"

    mock_redis = AsyncMock()
    mock_redis.get = AsyncMock(return_value="github")
    mock_redis.delete = AsyncMock()
    mock_redis.setex = AsyncMock()

    token_resp = MagicMock()
    token_resp.raise_for_status = MagicMock()
    token_resp.json = MagicMock(return_value={"access_token": "tok"})

    user_resp = MagicMock()
    user_resp.raise_for_status = MagicMock()
    user_resp.json = MagicMock(return_value={"id": 1111, "name": "No Email User", "email": None})

    email_resp = MagicMock()
    email_resp.raise_for_status = MagicMock()
    email_resp.json = MagicMock(return_value=[
        {"email": email, "primary": True, "verified": True},
    ])

    mock_client = AsyncMock()
    mock_client.post = AsyncMock(return_value=token_resp)
    mock_client.get = AsyncMock(side_effect=[user_resp, email_resp])
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)

    with patch("httpx.AsyncClient", return_value=mock_client):
        result = await handle_github_callback("code", "state", db_session, mock_redis)

    assert result.access_token


@pytest.mark.asyncio
async def test_github_callback_no_email_raises(db_session):
    """GitHub returns no email at all — raises 400."""
    from fastapi import HTTPException
    from src.services.oauth_service import handle_github_callback

    mock_redis = AsyncMock()
    mock_redis.get = AsyncMock(return_value="github")
    mock_redis.delete = AsyncMock()

    token_resp = MagicMock()
    token_resp.raise_for_status = MagicMock()
    token_resp.json = MagicMock(return_value={"access_token": "tok"})

    user_resp = MagicMock()
    user_resp.raise_for_status = MagicMock()
    user_resp.json = MagicMock(return_value={"id": 2222, "name": "No Email", "email": None})

    email_resp = MagicMock()
    email_resp.raise_for_status = MagicMock()
    email_resp.json = MagicMock(return_value=[])  # no verified primary

    mock_client = AsyncMock()
    mock_client.post = AsyncMock(return_value=token_resp)
    mock_client.get = AsyncMock(side_effect=[user_resp, email_resp])
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)

    with patch("httpx.AsyncClient", return_value=mock_client):
        with pytest.raises(HTTPException) as exc_info:
            await handle_github_callback("code", "state", db_session, mock_redis)
    assert exc_info.value.status_code == 400


# ─── WebSocket manager: dispatch + broadcast_all ──────────────────────────────

@pytest.mark.asyncio
async def test_ws_manager_broadcast_all():
    from src.services.ws_manager import ConnectionManager

    manager = ConnectionManager()

    ws1 = AsyncMock()
    ws1.accept = AsyncMock()
    ws1.send_text = AsyncMock()
    ws2 = AsyncMock()
    ws2.accept = AsyncMock()
    ws2.send_text = AsyncMock()

    await manager.connect("user1", ws1)
    await manager.connect("user2", ws2)

    await manager.broadcast_all({"type": "global_alert"})

    assert ws1.send_text.called
    assert ws2.send_text.called


@pytest.mark.asyncio
async def test_ws_manager_active_user_ids():
    from src.services.ws_manager import ConnectionManager

    manager = ConnectionManager()
    ws = AsyncMock()
    ws.accept = AsyncMock()
    await manager.connect("userA", ws)
    await manager.connect("userB", ws)

    active = manager.active_user_ids()
    assert "userA" in active
    assert "userB" in active


@pytest.mark.asyncio
async def test_ws_manager_disconnect_removes_user():
    from src.services.ws_manager import ConnectionManager

    manager = ConnectionManager()
    ws = AsyncMock()
    ws.accept = AsyncMock()

    await manager.connect("userX", ws)
    assert "userX" in manager.active_user_ids()

    await manager.disconnect("userX", ws)
    assert "userX" not in manager.active_user_ids()


@pytest.mark.asyncio
async def test_kafka_bridge_dispatch_with_subscribed_user():
    """KafkaBridgeConsumer._dispatch broadcasts to subscribed users."""
    from src.services.ws_manager import ConnectionManager, KafkaBridgeConsumer

    manager = ConnectionManager()
    ws = AsyncMock()
    ws.accept = AsyncMock()
    ws.send_text = AsyncMock()
    await manager.connect("user_dispatch", ws)

    bridge = KafkaBridgeConsumer(manager, "localhost:9092", "redis://localhost:6379")

    mock_redis = AsyncMock()
    mock_redis.smembers = AsyncMock(return_value={"AAPL", "*"})
    mock_redis.aclose = AsyncMock()

    with patch("redis.asyncio.from_url", return_value=mock_redis):
        await bridge._dispatch("AAPL", {"type": "market.ticks.clean", "data": {"symbol": "AAPL"}})

    ws.send_text.assert_called_once()


@pytest.mark.asyncio
async def test_kafka_bridge_dispatch_fallback_on_redis_error():
    """If Redis fails in _dispatch, falls back to broadcast_all."""
    from src.services.ws_manager import ConnectionManager, KafkaBridgeConsumer

    manager = ConnectionManager()
    ws = AsyncMock()
    ws.accept = AsyncMock()
    ws.send_text = AsyncMock()
    await manager.connect("user_fallback", ws)

    bridge = KafkaBridgeConsumer(manager, "localhost:9092", "redis://localhost:6379")

    with patch("redis.asyncio.from_url", side_effect=Exception("Redis down")):
        await bridge._dispatch("MSFT", {"type": "test", "data": {}})

    ws.send_text.assert_called_once()


def test_kafka_bridge_start_stop():
    """KafkaBridgeConsumer start creates a daemon thread."""
    from src.services.ws_manager import ConnectionManager, KafkaBridgeConsumer

    manager = ConnectionManager()
    bridge = KafkaBridgeConsumer(manager, "localhost:9092", "redis://localhost:6379")

    loop = asyncio.new_event_loop()
    try:
        bridge.start(loop)
        assert bridge._running is True
        assert bridge._thread is not None
        assert bridge._thread.daemon is True
    finally:
        bridge.stop()
        assert bridge._running is False
        loop.close()


# ─── Analytics: risk endpoint ─────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_analytics_risk_success(client: AsyncClient, free_user: User):
    mock_resp = MagicMock()
    mock_resp.raise_for_status = MagicMock()
    mock_resp.json = MagicMock(return_value={"symbols": ["AAPL"], "var_95": -0.02})

    mock_client = AsyncMock()
    mock_client.get = AsyncMock(return_value=mock_resp)
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)

    with patch("httpx.AsyncClient", return_value=mock_client):
        resp = await client.get(
            "/api/v1/analytics/risk?symbols=AAPL",
            headers=auth_headers(free_user),
        )

    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_analytics_risk_premium_user(client: AsyncClient, premium_user: User):
    """Premium user gets full=true flag in downstream call."""
    mock_resp = MagicMock()
    mock_resp.raise_for_status = MagicMock()
    mock_resp.json = MagicMock(return_value={"symbols": ["MSFT"], "var_95": -0.015, "full": True})

    mock_client = AsyncMock()
    mock_client.get = AsyncMock(return_value=mock_resp)
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)

    with patch("httpx.AsyncClient", return_value=mock_client):
        resp = await client.get(
            "/api/v1/analytics/risk?symbols=MSFT",
            headers=auth_headers(premium_user),
        )

    assert resp.status_code == 200
    # Verify the downstream call used full=true
    call_kwargs = mock_client.get.call_args
    assert call_kwargs.kwargs["params"]["full"] == "true"


@pytest.mark.asyncio
async def test_analytics_risk_502_on_error(client: AsyncClient, free_user: User):
    import httpx as _httpx

    mock_client = AsyncMock()
    mock_client.get = AsyncMock(side_effect=_httpx.ConnectError("refused"))
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)

    with patch("httpx.AsyncClient", return_value=mock_client):
        resp = await client.get(
            "/api/v1/analytics/risk?symbols=AAPL",
            headers=auth_headers(free_user),
        )

    assert resp.status_code == 502


# ─── Analytics: portfolio + backtest with mocked TimescaleDB data ─────────────

@pytest.mark.asyncio
async def test_analytics_portfolio_with_data(db_session, premium_user: User):
    """Directly call the portfolio logic with a mocked session."""
    from src.api.v1.analytics import get_portfolio

    rows = [
        {"timestamp": datetime.now(timezone.utc) - timedelta(hours=i), "close": 150.0 + i}
        for i in range(5)
    ]
    mock_result = MagicMock()
    mock_result.mappings.return_value.all.return_value = rows

    mock_session = AsyncMock()
    mock_session.execute = AsyncMock(return_value=mock_result)

    result = await get_portfolio(
        symbols="AAPL,MSFT",
        period="7d",
        user=premium_user,
        db=mock_session,
    )
    assert "portfolio" in result
    assert result["period"] == "7d"


@pytest.mark.asyncio
async def test_analytics_backtest_with_data(db_session, premium_user: User):
    """Directly call the backtest logic with enough mock rows."""
    from src.api.v1.analytics import run_backtest

    rows = [
        {"timestamp": datetime.now(timezone.utc) - timedelta(minutes=i), "close": 100.0 + (i % 10)}
        for i in range(100)
    ]
    mock_result = MagicMock()
    mock_result.mappings.return_value.all.return_value = rows

    mock_session = AsyncMock()
    mock_session.execute = AsyncMock(return_value=mock_result)

    result = await run_backtest(
        body={"symbol": "AAPL", "short_window": 5, "long_window": 20, "initial_capital": 10000},
        user=premium_user,
        db=mock_session,
    )
    assert "final_value" in result
    assert "sharpe_ratio" in result
    assert result["symbol"] == "AAPL"


# ─── Middleware rate_limit key functions ──────────────────────────────────────

def test_rate_limit_key_functions():
    from src.middleware.rate_limit import _rate_limit_key, _login_key, limiter

    # Authenticated request — keyed by user.id
    mock_user = MagicMock()
    mock_user.id = "user-123"
    mock_req = MagicMock()
    mock_req.state.user = mock_user
    key = _rate_limit_key(mock_req)
    assert "user-123" in key

    # Unauthenticated request — keyed by IP
    mock_req_anon = MagicMock()
    mock_req_anon.state.user = None
    mock_req_anon.client.host = "1.2.3.4"
    mock_req_anon.headers = {}
    anon_key = _rate_limit_key(mock_req_anon)
    assert "ip:" in anon_key

    # Login key uses client IP
    mock_req2 = MagicMock()
    mock_req2.client.host = "127.0.0.1"
    mock_req2.headers = {}
    login_key = _login_key(mock_req2)
    assert "login:" in login_key

    assert limiter is not None


# ─── Security: SSRF validator ─────────────────────────────────────────────────

def test_ssrf_blocks_private_ips():
    from src.core.security import validate_webhook_url

    private_urls = [
        "http://192.168.1.1/steal",
        "http://10.0.0.1/internal",
        "http://172.16.0.1/api",
        "http://localhost/hook",
        "http://127.0.0.1/hook",
    ]
    for url in private_urls:
        result = validate_webhook_url(url)
        assert result is False, f"Expected False for {url}"


def test_ssrf_allows_public_urls():
    from src.core.security import validate_webhook_url

    assert validate_webhook_url("https://hooks.example.com/notify") is True
    assert validate_webhook_url("https://api.slack.com/webhook/abc") is True


def test_ssrf_invalid_scheme():
    from src.core.security import validate_webhook_url

    assert validate_webhook_url("ftp://example.com/hook") is False
    assert validate_webhook_url("file:///etc/passwd") is False


# ─── Dependencies: optional_user ─────────────────────────────────────────────

@pytest.mark.asyncio
async def test_optional_user_returns_none_on_no_creds():
    from fastapi import Request
    from src.core.dependencies import optional_user
    from sqlalchemy.ext.asyncio import AsyncSession

    mock_req = MagicMock(spec=Request)
    mock_db = AsyncMock(spec=AsyncSession)

    result = await optional_user(mock_req, None, mock_db)
    assert result is None


@pytest.mark.asyncio
async def test_optional_user_returns_none_on_bad_token():
    from fastapi import Request
    from fastapi.security import HTTPAuthorizationCredentials
    from src.core.dependencies import optional_user
    from sqlalchemy.ext.asyncio import AsyncSession

    mock_req = MagicMock(spec=Request)
    mock_db = AsyncMock(spec=AsyncSession)
    creds = HTTPAuthorizationCredentials(scheme="Bearer", credentials="invalid.token.here")

    result = await optional_user(mock_req, creds, mock_db)
    assert result is None


# ─── Auth: lockout ────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_login_locked_account(db_session):
    """Locked account returns 423."""
    from fastapi import HTTPException
    from src.core.security import hash_password
    from src.models.user import User
    from src.services.auth_service import login_user
    from src.schemas.auth import LoginRequest

    locked_until = datetime.now(timezone.utc) + timedelta(minutes=30)
    user = User(
        id=uuid.uuid4(),
        email=f"locked_{uuid.uuid4().hex[:8]}@t.com",
        hashed_password=hash_password("correct"),
        role="free_user",
        locked_until=locked_until,
    )
    db_session.add(user)
    await db_session.flush()

    mock_redis = AsyncMock()
    req = LoginRequest(email=user.email, password="correct")
    with pytest.raises(HTTPException) as exc_info:
        await login_user(req, db_session, mock_redis)
    assert exc_info.value.status_code == 423


