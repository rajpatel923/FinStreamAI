import time

import redis.asyncio as aioredis
import sqlalchemy
from fastapi import APIRouter
from fastapi.responses import JSONResponse

from src.core.config import settings
from src.core.database import postgres_engine, timescaledb_engine
from src.core.logging import get_logger

router = APIRouter(tags=["health"])
logger = get_logger(__name__)

_start_time = time.time()


@router.get("/live")
async def liveness() -> JSONResponse:
    """Liveness probe — confirms the app process is running."""
    return JSONResponse({"status": "ok"})


@router.get("/ready")
async def readiness() -> JSONResponse:
    """Readiness probe — confirms all critical dependencies are reachable."""
    checks = await _run_checks()
    all_ok = all(v == "ok" for v in checks.values())
    status_code = 200 if all_ok else 503
    return JSONResponse(
        {"status": "ready" if all_ok else "not_ready", **checks},
        status_code=status_code,
    )


@router.get("/health")
async def health() -> JSONResponse:
    """Full health check with dependency status and uptime."""
    checks = await _run_checks()
    all_ok = all(v == "ok" for v in checks.values())
    return JSONResponse(
        {
            "status": "healthy" if all_ok else "degraded",
            "uptime_seconds": round(time.time() - _start_time, 2),
            "dependencies": checks,
        },
        status_code=200 if all_ok else 503,
    )


async def _run_checks() -> dict[str, str]:
    return {
        "postgres": await _check_postgres(),
        "timescaledb": await _check_timescaledb(),
        "redis": await _check_redis(),
    }


async def _check_postgres() -> str:
    try:
        async with postgres_engine.connect() as conn:
            await conn.execute(sqlalchemy.text("SELECT 1"))
        return "ok"
    except Exception as exc:
        logger.warning("postgres health check failed", error=str(exc))
        return "error"


async def _check_timescaledb() -> str:
    try:
        async with timescaledb_engine.connect() as conn:
            await conn.execute(sqlalchemy.text("SELECT 1"))
        return "ok"
    except Exception as exc:
        logger.warning("timescaledb health check failed", error=str(exc))
        return "error"


async def _check_redis() -> str:
    try:
        client = aioredis.from_url(settings.redis_url, socket_connect_timeout=2)
        await client.ping()
        await client.aclose()
        return "ok"
    except Exception as exc:
        logger.warning("redis health check failed", error=str(exc))
        return "error"
