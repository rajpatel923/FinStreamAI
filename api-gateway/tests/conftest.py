"""Shared test fixtures for api-gateway."""
from __future__ import annotations

import asyncio
import uuid
from datetime import datetime, timezone
from typing import AsyncGenerator
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

from src.core.database import Base, get_db, get_timescale_db
from src.core.config import settings
from src.core.security import create_access_token, hash_password
from src.models.user import User
from src.models.alert import Alert, ExportJob

# ─── In-memory SQLite engine for tests ───────────────────────────────────────
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


@pytest_asyncio.fixture
async def client(db_session, mock_redis) -> AsyncGenerator[AsyncClient, None]:
    from src.main import app
    from src.core.dependencies import get_redis

    async def _override_db():
        yield db_session

    def _override_redis():
        return mock_redis

    app.dependency_overrides[get_db] = _override_db
    app.dependency_overrides[get_timescale_db] = _override_db
    app.dependency_overrides[get_redis] = _override_redis

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        yield ac

    app.dependency_overrides.clear()


@pytest_asyncio.fixture
async def free_user(db_session: AsyncSession) -> User:
    user = User(
        id=uuid.uuid4(),
        email=f"free_{uuid.uuid4().hex[:8]}@test.com",
        hashed_password=hash_password("password123"),
        role="free_user",
    )
    db_session.add(user)
    await db_session.flush()
    return user


@pytest_asyncio.fixture
async def premium_user(db_session: AsyncSession) -> User:
    user = User(
        id=uuid.uuid4(),
        email=f"premium_{uuid.uuid4().hex[:8]}@test.com",
        hashed_password=hash_password("password123"),
        role="premium_user",
    )
    db_session.add(user)
    await db_session.flush()
    return user


@pytest_asyncio.fixture
async def admin_user(db_session: AsyncSession) -> User:
    user = User(
        id=uuid.uuid4(),
        email=f"admin_{uuid.uuid4().hex[:8]}@test.com",
        hashed_password=hash_password("password123"),
        role="admin",
    )
    db_session.add(user)
    await db_session.flush()
    return user


def make_token(user: User) -> str:
    return create_access_token(str(user.id), user.role)


def auth_headers(user: User) -> dict:
    return {"Authorization": f"Bearer {make_token(user)}"}


@pytest.fixture
def mock_redis():
    mock = AsyncMock()
    mock.get = AsyncMock(return_value=None)
    mock.set = AsyncMock(return_value=True)
    mock.setex = AsyncMock(return_value=True)
    mock.delete = AsyncMock(return_value=1)
    mock.smembers = AsyncMock(return_value=set())
    mock.sadd = AsyncMock(return_value=1)
    mock.expire = AsyncMock(return_value=True)
    mock.aclose = AsyncMock()
    return mock
