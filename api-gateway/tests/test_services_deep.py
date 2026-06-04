"""Deep service unit tests: query_service, export_service, ws_manager, oauth, dependencies."""
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.security import hash_password
from src.models.user import ApiKey, User


# ─── Query service ────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_query_market_data_mock(db_session: AsyncSession):
    from src.services.query_service import query_market_data

    mock_result = MagicMock()
    mock_result.mappings.return_value.all.return_value = [
        {"timestamp": datetime(2026, 1, 1, tzinfo=timezone.utc), "symbol": "AAPL",
         "open": 100.0, "high": 105.0, "low": 99.0, "close": 103.0, "volume": 1000000, "vwap": 102.0},
    ]
    db_session.execute = AsyncMock(return_value=mock_result)

    result = await query_market_data(db_session, "AAPL", limit=100)
    assert result["symbol"] == "AAPL"
    assert len(result["data"]) == 1


@pytest.mark.asyncio
async def test_query_market_data_with_cursor(db_session: AsyncSession):
    from src.services.query_service import query_market_data

    mock_result = MagicMock()
    mock_result.mappings.return_value.all.return_value = []
    db_session.execute = AsyncMock(return_value=mock_result)

    result = await query_market_data(db_session, "MSFT", cursor="2026-01-01T00:00:00+00:00", limit=10)
    assert result["symbol"] == "MSFT"
    assert result["cursor"] is None


@pytest.mark.asyncio
async def test_query_market_data_cursor_returned(db_session: AsyncSession):
    from src.services.query_service import query_market_data

    rows = [
        {"timestamp": datetime(2026, 1, i, tzinfo=timezone.utc), "symbol": "AAPL",
         "open": 100.0, "high": 105.0, "low": 99.0, "close": 103.0, "volume": 1000000, "vwap": 102.0}
        for i in range(1, 6)
    ]
    mock_result = MagicMock()
    mock_result.mappings.return_value.all.return_value = rows
    db_session.execute = AsyncMock(return_value=mock_result)

    result = await query_market_data(db_session, "AAPL", limit=5)
    # When result count == limit, cursor is returned
    assert result["cursor"] is not None


@pytest.mark.asyncio
async def test_query_sentiment_mock(db_session: AsyncSession):
    from src.services.query_service import query_sentiment

    mock_result = MagicMock()
    mock_result.mappings.return_value.all.return_value = [
        {"timestamp": datetime(2026, 1, 1, tzinfo=timezone.utc), "symbol": "AAPL",
         "sentiment_score": 0.8, "sentiment_label": "positive", "source": "news"},
    ]
    db_session.execute = AsyncMock(return_value=mock_result)

    result = await query_sentiment(db_session, "AAPL")
    assert len(result["data"]) == 1


@pytest.mark.asyncio
async def test_run_custom_query_valid(db_session: AsyncSession):
    from src.services.query_service import run_custom_query

    mock_result = MagicMock()
    mock_result.mappings.return_value.all.return_value = []
    db_session.execute = AsyncMock(return_value=mock_result)

    result = await run_custom_query(
        db_session, "market_ticks", "AAPL", None, None, 100, {}, "timestamp", "desc"
    )
    assert result["row_count"] == 0


@pytest.mark.asyncio
async def test_run_custom_query_invalid_table(db_session: AsyncSession):
    from src.services.query_service import run_custom_query

    with pytest.raises(ValueError, match="not queryable"):
        await run_custom_query(db_session, "secret_table", None, None, None, 100, {}, "timestamp", "desc")


@pytest.mark.asyncio
async def test_run_custom_query_invalid_order_dir(db_session: AsyncSession):
    from src.services.query_service import run_custom_query

    with pytest.raises(ValueError, match="order_dir"):
        await run_custom_query(db_session, "market_ticks", None, None, None, 100, {}, "timestamp", "random")


@pytest.mark.asyncio
async def test_run_custom_query_invalid_order_by(db_session: AsyncSession):
    from src.services.query_service import run_custom_query

    with pytest.raises(ValueError, match="alphanumeric"):
        await run_custom_query(db_session, "market_ticks", None, None, None, 100, {}, "ts; DROP TABLE", "desc")


# ─── WebSocket manager ────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_ws_manager_broadcast_handles_dead_connection():
    from src.services.ws_manager import ConnectionManager
    manager = ConnectionManager()

    ws_ok = AsyncMock()
    ws_ok.accept = AsyncMock()
    ws_ok.send_text = AsyncMock()

    ws_bad = AsyncMock()
    ws_bad.accept = AsyncMock()
    ws_bad.send_text = AsyncMock(side_effect=Exception("connection closed"))

    await manager.connect("user1", ws_ok)
    await manager.connect("user1", ws_bad)

    await manager.broadcast("user1", {"type": "test"})

    ws_ok.send_text.assert_called_once()
    # Dead connection should have been removed
    assert ws_bad not in manager._connections.get("user1", [])


@pytest.mark.asyncio
async def test_ws_manager_broadcast_unknown_user():
    from src.services.ws_manager import ConnectionManager
    manager = ConnectionManager()
    # Should not raise for unknown user
    await manager.broadcast("nonexistent", {"type": "test"})


def test_ws_bridge_consumer_init():
    from src.services.ws_manager import ConnectionManager, KafkaBridgeConsumer
    manager = ConnectionManager()
    bridge = KafkaBridgeConsumer(manager, "localhost:9092", "redis://localhost:6379")
    assert not bridge._running
    bridge.stop()  # should not raise when not started


# ─── Dependencies — API key path ─────────────────────────────────────────────

@pytest.mark.asyncio
async def test_dependencies_api_key_user(db_session: AsyncSession):
    from src.core.dependencies import _user_from_api_key
    from src.core.security import generate_api_key

    user = User(id=uuid.uuid4(), email=f"ak_{uuid.uuid4().hex[:8]}@t.com", role="premium_user")
    db_session.add(user)
    await db_session.flush()

    full_key, prefix, key_hash = generate_api_key()
    api_key = ApiKey(
        user_id=user.id,
        key_prefix=prefix,
        key_hash=key_hash,
        name="Test Key",
    )
    db_session.add(api_key)
    await db_session.flush()

    mock_redis = AsyncMock()
    mock_redis.get = AsyncMock(return_value=None)  # cache miss
    mock_redis.setex = AsyncMock(return_value=True)

    result = await _user_from_api_key(full_key, db_session, mock_redis)
    assert result.id == user.id


@pytest.mark.asyncio
async def test_dependencies_api_key_invalid(db_session: AsyncSession):
    from fastapi import HTTPException
    from src.core.dependencies import _user_from_api_key

    mock_redis = AsyncMock()
    mock_redis.get = AsyncMock(return_value=None)

    with pytest.raises(HTTPException) as exc_info:
        await _user_from_api_key("fsk_invalid_key_prefix_here", db_session, mock_redis)
    assert exc_info.value.status_code == 401


@pytest.mark.asyncio
async def test_dependencies_api_key_cached(db_session: AsyncSession):
    from src.core.dependencies import _user_from_api_key

    user = User(id=uuid.uuid4(), email=f"cache_{uuid.uuid4().hex[:8]}@t.com", role="free_user")
    db_session.add(user)
    await db_session.flush()

    import uuid as _uuid_mod
    mock_redis = AsyncMock()
    mock_redis.get = AsyncMock(return_value=str(user.id))  # cache hit

    result = await _user_from_api_key("fsk_testprefix", db_session, mock_redis)
    assert str(result.id) == str(user.id)


# ─── OAuth service ────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_oauth_upsert_updates_full_name(db_session: AsyncSession):
    from src.services.oauth_service import _upsert_oauth_user

    email = f"update_{uuid.uuid4().hex[:8]}@test.com"
    existing = User(id=uuid.uuid4(), email=email, role="free_user")
    db_session.add(existing)
    await db_session.flush()

    user = await _upsert_oauth_user(db_session, email, "google", "sub456", "New Name")
    assert user.full_name == "New Name"


@pytest.mark.asyncio
async def test_oauth_state_expires(mock_redis):
    from fastapi import HTTPException
    from src.services.oauth_service import _validate_state
    mock_redis.get = AsyncMock(return_value=None)
    with pytest.raises(HTTPException) as exc_info:
        await _validate_state("expired_state", mock_redis)
    assert exc_info.value.status_code == 400


# ─── Middleware auth (JWTAuthMiddleware) ─────────────────────────────────────

def test_correlation_id_middleware_exists():
    """Verify CorrelationIdMiddleware is importable."""
    from src.middleware.logging import CorrelationIdMiddleware
    assert CorrelationIdMiddleware is not None


def test_limiter_exists():
    """Verify slowapi limiter is importable."""
    from src.middleware.rate_limit import limiter, _rate_limit_key, _login_key
    assert limiter is not None
    assert callable(_rate_limit_key)
    assert callable(_login_key)


# ─── Export service unit ─────────────────────────────────────────────────────

def _make_mock_session_factory(rows):
    """Create a properly async session factory mock."""
    mock_mappings = MagicMock()
    mock_mappings.all.return_value = rows

    mock_result = MagicMock()
    mock_result.mappings.return_value = mock_mappings

    mock_session = AsyncMock()
    mock_session.execute = AsyncMock(return_value=mock_result)
    mock_session.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session.__aexit__ = AsyncMock(return_value=False)

    class MockFactory:
        def __call__(self):
            return mock_session

    return MockFactory()


@pytest.mark.asyncio
async def test_export_service_mocked():
    from src.services.export_service import run_export

    rows = [{"timestamp": datetime(2026, 1, 1, tzinfo=timezone.utc), "symbol": "AAPL", "close": 100.0}]
    factory = _make_mock_session_factory(rows)

    mock_s3 = MagicMock()
    mock_s3.upload_fileobj = MagicMock()
    mock_s3.generate_presigned_url = MagicMock(return_value="https://example.com/file.json")

    with patch("src.services.export_service._get_s3_client", return_value=mock_s3):
        result = await run_export(
            job_id=uuid.uuid4(),
            user_id=uuid.uuid4(),
            query_params={"table": "market_bars_1min", "symbol": "AAPL"},
            output_format="json",
            db_session_factory=factory,
        )
    assert result["status"] == "done"
    assert result["row_count"] == 1
    assert "download_url" in result


@pytest.mark.asyncio
async def test_export_service_csv_format():
    from src.services.export_service import run_export

    rows = [{"timestamp": datetime(2026, 1, 1, tzinfo=timezone.utc), "symbol": "AAPL", "close": 100.0}]
    factory = _make_mock_session_factory(rows)

    mock_s3 = MagicMock()
    mock_s3.upload_fileobj = MagicMock()
    mock_s3.generate_presigned_url = MagicMock(return_value="https://example.com/file.csv")

    with patch("src.services.export_service._get_s3_client", return_value=mock_s3):
        result = await run_export(
            job_id=uuid.uuid4(),
            user_id=uuid.uuid4(),
            query_params={"table": "market_bars_1min"},
            output_format="csv",
            db_session_factory=factory,
        )
    assert result["status"] == "done"


@pytest.mark.asyncio
async def test_export_service_parquet_format():
    from src.services.export_service import run_export

    rows = [{"timestamp": datetime(2026, 1, 1, tzinfo=timezone.utc), "symbol": "AAPL", "close": 100.0}]
    factory = _make_mock_session_factory(rows)

    mock_s3 = MagicMock()
    mock_s3.upload_fileobj = MagicMock()
    mock_s3.generate_presigned_url = MagicMock(return_value="https://example.com/file.parquet")

    with patch("src.services.export_service._get_s3_client", return_value=mock_s3):
        result = await run_export(
            job_id=uuid.uuid4(),
            user_id=uuid.uuid4(),
            query_params={"table": "market_bars_1min"},
            output_format="parquet",
            db_session_factory=factory,
        )
    assert result["status"] == "done"


@pytest.mark.asyncio
async def test_export_service_error_handling():
    from src.services.export_service import run_export

    class BrokenFactory:
        def __call__(self):
            raise RuntimeError("DB down")

    result = await run_export(
        job_id=uuid.uuid4(),
        user_id=uuid.uuid4(),
        query_params={},
        output_format="json",
        db_session_factory=BrokenFactory(),
    )
    assert result["status"] == "failed"
    assert "error_message" in result
