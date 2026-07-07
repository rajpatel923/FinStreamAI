"""Async SQLAlchemy engine for api-gateway (PostgreSQL only)."""
from collections.abc import AsyncGenerator

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase

from .config import settings


class Base(DeclarativeBase):
    pass


postgres_engine = create_async_engine(
    settings.postgres_url,
    pool_size=10,
    max_overflow=20,
    pool_pre_ping=True,
    echo=False,
)

timescaledb_engine = create_async_engine(
    settings.timescaledb_url,
    pool_size=5,
    max_overflow=10,
    pool_pre_ping=True,
    echo=False,
)

AsyncPostgresSession = async_sessionmaker(
    postgres_engine,
    class_=AsyncSession,
    expire_on_commit=False,
)

AsyncTimescaleSession = async_sessionmaker(
    timescaledb_engine,
    class_=AsyncSession,
    expire_on_commit=False,
)


async def init_db() -> None:
    import sqlalchemy

    async with postgres_engine.connect() as conn:
        await conn.execute(sqlalchemy.text("SELECT 1"))


async def close_db() -> None:
    await postgres_engine.dispose()
    await timescaledb_engine.dispose()


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    async with AsyncPostgresSession() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise


async def get_timescale_db() -> AsyncGenerator[AsyncSession, None]:
    async with AsyncTimescaleSession() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
