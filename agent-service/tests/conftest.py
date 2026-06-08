"""Shared test fixtures for agent-service."""
from __future__ import annotations

import sys
import uuid
from pathlib import Path
from typing import AsyncGenerator
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

# ─── Path setup ──────────────────────────────────────────────────────────────
_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

_SERVICE_ROOT = Path(__file__).resolve().parents[1]
if str(_SERVICE_ROOT) not in sys.path:
    sys.path.insert(0, str(_SERVICE_ROOT))

from src.core.database import Base, get_db  # noqa: E402
from src.core.config import settings  # noqa: E402
from src.core.dependencies import get_redis  # noqa: E402
from src.models.user import User  # noqa: E402
from src.models.watchlist import WatchlistItem  # noqa: E402
from src.models.preferences import UserPreference  # noqa: E402
from src.models.portfolio import PortfolioPosition  # noqa: E402
from src.models.trading import TradeOrder  # noqa: E402
from src.models.conversation import AgentConversation, AgentMessage  # noqa: E402

# ─── Security helpers (reuse api-gateway's create_access_token) ───────────────
_GW_SRC = _REPO_ROOT / "api-gateway"
if str(_GW_SRC) not in sys.path:
    sys.path.insert(0, str(_GW_SRC))


def _make_token(user: User) -> str:
    from jose import jwt
    from datetime import datetime, timedelta, timezone

    # Use same key as settings (empty string works for tests — decoded with same key)
    secret = settings.JWT_SECRET_KEY or ""
    payload = {
        "sub": str(user.id),
        "role": user.role,
        "jti": str(uuid.uuid4()),
        "iat": datetime.now(timezone.utc),
        "exp": datetime.now(timezone.utc) + timedelta(minutes=30),
    }
    return jwt.encode(payload, secret, algorithm="HS256")


def auth_headers(user: User) -> dict:
    return {"Authorization": f"Bearer {_make_token(user)}"}


# ─── Patch JWT_SECRET_KEY for tests ──────────────────────────────────────────
import os
os.environ.setdefault("JWT_SECRET_KEY", "test-secret-key-for-agent-service-tests")

# ─── SQLite in-memory test engine ────────────────────────────────────────────
TEST_DB_URL = "sqlite+aiosqlite:///:memory:"

_test_engine = create_async_engine(
    TEST_DB_URL,
    connect_args={"check_same_thread": False},
    poolclass=StaticPool,
)
_TestSession = async_sessionmaker(_test_engine, class_=AsyncSession, expire_on_commit=False)


@pytest_asyncio.fixture(autouse=True)
async def create_tables():
    async with _test_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield
    async with _test_engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)


@pytest_asyncio.fixture
async def db_session(create_tables) -> AsyncGenerator[AsyncSession, None]:
    async with _TestSession() as session:
        yield session
        await session.rollback()


# ─── Mock Redis ───────────────────────────────────────────────────────────────
@pytest.fixture
def mock_redis():
    mock = AsyncMock()
    mock.get = AsyncMock(return_value=None)
    mock.set = AsyncMock(return_value=True)
    mock.setex = AsyncMock(return_value=True)
    mock.delete = AsyncMock(return_value=1)
    mock.smembers = AsyncMock(return_value=set())
    mock.sadd = AsyncMock(return_value=1)
    mock.srem = AsyncMock(return_value=1)
    mock.expire = AsyncMock(return_value=True)
    mock.lpush = AsyncMock(return_value=1)
    mock.blpop = AsyncMock(return_value=None)
    mock.hget = AsyncMock(return_value=None)
    mock.aclose = AsyncMock()
    return mock


# ─── Mock ChatAnthropic ───────────────────────────────────────────────────────
@pytest.fixture
def mock_chat_anthropic():
    mock = MagicMock()

    async def _astream(messages, **kwargs):
        chunk1 = MagicMock()
        chunk1.content = "Based on current market analysis, "
        chunk2 = MagicMock()
        chunk2.content = "I recommend diversifying your portfolio."
        for chunk in [chunk1, chunk2]:
            yield chunk

    async def _ainvoke(messages, **kwargs):
        resp = MagicMock()
        resp.content = '{"intent": "portfolio_advice"}'
        return resp

    mock.astream = _astream
    mock.ainvoke = _ainvoke
    return mock


# ─── Mock Alpaca ──────────────────────────────────────────────────────────────
@pytest.fixture
def mock_alpaca():
    with patch("alpaca.trading.client.TradingClient") as mock_cls:
        instance = MagicMock()
        order = MagicMock()
        order.id = str(uuid.uuid4())
        order.symbol = "AAPL"
        order.qty = 10.0
        order.side = "buy"
        order.status = "accepted"
        instance.submit_order.return_value = order
        instance.get_order_by_id.return_value = order
        account = MagicMock()
        account.id = str(uuid.uuid4())
        account.equity = "10000.00"
        account.buying_power = "5000.00"
        account.cash = "5000.00"
        instance.get_account.return_value = account
        instance.get_all_positions.return_value = []
        mock_cls.return_value = instance
        yield instance


# ─── Mock Checkpointer ────────────────────────────────────────────────────────
@pytest.fixture
def mock_checkpointer():
    from langgraph.checkpoint.memory import MemorySaver
    return MemorySaver()


# ─── Mock Scheduler (autouse) ────────────────────────────────────────────────
@pytest.fixture(autouse=True)
def mock_scheduler():
    with patch("src.services.monitoring_loop.start_scheduler", return_value=None):
        yield


# ─── Users ────────────────────────────────────────────────────────────────────
@pytest_asyncio.fixture
async def free_user(db_session: AsyncSession) -> User:
    user = User(
        id=uuid.uuid4(),
        email=f"free_{uuid.uuid4().hex[:8]}@test.com",
        hashed_password="$2b$12$LQv3c1yqBWVHxkd0LHAkCOYz6TtxMQJqhN8/LewdBPj4J/sFzBJve",
        role="free_user",
        is_active=True,
    )
    db_session.add(user)
    await db_session.flush()
    return user


@pytest_asyncio.fixture
async def premium_user(db_session: AsyncSession) -> User:
    user = User(
        id=uuid.uuid4(),
        email=f"premium_{uuid.uuid4().hex[:8]}@test.com",
        hashed_password="$2b$12$LQv3c1yqBWVHxkd0LHAkCOYz6TtxMQJqhN8/LewdBPj4J/sFzBJve",
        role="premium_user",
        is_active=True,
    )
    db_session.add(user)
    await db_session.flush()
    return user


# ─── HTTP Test Client ─────────────────────────────────────────────────────────
@pytest_asyncio.fixture
async def client(db_session, mock_redis) -> AsyncGenerator[AsyncClient, None]:
    from src.main import app

    async def _override_db():
        yield db_session

    def _override_redis():
        return mock_redis

    app.dependency_overrides[get_db] = _override_db
    app.dependency_overrides[get_redis] = _override_redis

    # Inject mock supervisor/chat_model into app.state
    app.state.supervisor = None
    app.state.chat_model = None

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        yield ac

    app.dependency_overrides.clear()
