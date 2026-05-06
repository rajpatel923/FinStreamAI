from collections.abc import AsyncGenerator

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase

from .config import settings
from .logging import get_logger

logger = get_logger(__name__)


class Base(DeclarativeBase):
    pass


# PostgreSQL engine (user/auth/ML metadata)
postgres_engine = create_async_engine(
    settings.postgres_url,
    pool_size=10,
    max_overflow=20,
    pool_pre_ping=True,
    echo=settings.DEBUG,
)

# TimescaleDB engine (time-series market data)
timescaledb_engine = create_async_engine(
    settings.timescaledb_url,
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

AsyncTimescaleSession = async_sessionmaker(
    timescaledb_engine,
    class_=AsyncSession,
    expire_on_commit=False,
)


async def init_db() -> None:
    logger.info("Initializing database connections")
    # Verify connectivity on startup
    async with postgres_engine.connect() as conn:
        await conn.execute(__import__("sqlalchemy").text("SELECT 1"))
    logger.info("PostgreSQL connection verified")

    async with timescaledb_engine.connect() as conn:
        await conn.execute(__import__("sqlalchemy").text("SELECT 1"))
    logger.info("TimescaleDB connection verified")


async def close_db() -> None:
    logger.info("Closing database connections")
    await postgres_engine.dispose()
    await timescaledb_engine.dispose()


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """FastAPI dependency — yields a PostgreSQL session."""
    async with AsyncPostgresSession() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise


async def get_timescale_db() -> AsyncGenerator[AsyncSession, None]:
    """FastAPI dependency — yields a TimescaleDB session."""
    async with AsyncTimescaleSession() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
