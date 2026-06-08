"""APScheduler-based monitoring loop."""
from __future__ import annotations

import structlog

logger = structlog.get_logger(__name__)


def start_scheduler(app_state, settings):
    """Start APScheduler with watchlist scan and daily digest."""
    try:
        from apscheduler.schedulers.asyncio import AsyncIOScheduler

        scheduler = AsyncIOScheduler()

        scheduler.add_job(
            _run_scan_job,
            "interval",
            minutes=settings.WATCHLIST_MONITOR_INTERVAL_MINUTES,
            id="watchlist_scan",
            args=[app_state, settings],
        )

        scheduler.add_job(
            _run_digest_job,
            "cron",
            hour=settings.DAILY_DIGEST_HOUR,
            minute=0,
            id="daily_digest",
            args=[app_state, settings],
        )

        scheduler.start()
        logger.info(
            "Monitoring scheduler started",
            scan_interval_min=settings.WATCHLIST_MONITOR_INTERVAL_MINUTES,
            digest_hour=settings.DAILY_DIGEST_HOUR,
        )
        return scheduler
    except ImportError:
        logger.warning("apscheduler not installed — monitoring disabled")
        return None


async def stop_scheduler(scheduler) -> None:
    if scheduler is not None:
        try:
            scheduler.shutdown(wait=False)
        except Exception as exc:
            logger.warning("Scheduler shutdown error", error=str(exc))


async def _run_scan_job(app_state, settings) -> None:
    try:
        from src.core.database import AsyncPostgresSession
        from src.agents.watchlist_monitor import run_watchlist_scan
        import redis.asyncio as aioredis

        redis = aioredis.from_url(settings.redis_url, decode_responses=True)
        async with AsyncPostgresSession() as db:
            count = await run_watchlist_scan(db, redis, settings)
            if count:
                logger.info("Watchlist scan sent notifications", count=count)
        await redis.aclose()
    except Exception as exc:
        logger.error("Watchlist scan job failed", error=str(exc))


async def _run_digest_job(app_state, settings) -> None:
    try:
        from src.core.database import AsyncPostgresSession
        from src.services.digest_service import send_daily_digest

        async with AsyncPostgresSession() as db:
            await send_daily_digest(db, settings)
    except Exception as exc:
        logger.error("Daily digest job failed", error=str(exc))
