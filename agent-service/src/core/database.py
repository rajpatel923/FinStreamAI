"""Async SQLAlchemy engine for agent-service (PostgreSQL)."""
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
    echo=settings.DEBUG,
)

AsyncPostgresSession = async_sessionmaker(
    postgres_engine,
    class_=AsyncSession,
    expire_on_commit=False,
)


async def init_db() -> None:
    import sqlalchemy

    async with postgres_engine.connect() as conn:
        await conn.execute(sqlalchemy.text("SELECT 1"))


async def close_db() -> None:
    await postgres_engine.dispose()


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    async with AsyncPostgresSession() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
