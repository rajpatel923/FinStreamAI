"""Watchlist monitor — scans signals and pushes recommendations."""
from __future__ import annotations

import json
from datetime import date

import httpx
import structlog
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.models.watchlist import WatchlistItem

logger = structlog.get_logger(__name__)


async def run_watchlist_scan(db: AsyncSession, redis, settings) -> int:
    """Scan watchlist items and push recommendations if signal_strength is high.

    Returns count of notifications sent.
    """
    result = await db.execute(
        select(WatchlistItem).where(WatchlistItem.alert_on_signal == True)  # noqa: E712
    )
    items = result.scalars().all()

    sent = 0
    today = date.today().isoformat()

    for item in items:
        symbol = item.symbol
        user_id = str(item.user_id)

        # Dedup check
        dedup_key = f"agent:monitor:dedup:{user_id}:{symbol}:{today}"
        already_sent = await redis.get(dedup_key)
        if already_sent:
            continue

        # Check signal strength from Redis
        try:
            signal_raw = await redis.hget(f"finstreami:signals:{symbol}", "signal_strength")
            signal_strength = float(signal_raw) if signal_raw else 0.0
        except Exception:
            signal_strength = 0.0

        if signal_strength < 0.7:
            continue

        # Mark as sent (dedup)
        await redis.setex(dedup_key, 86400, "1")

        # Produce to agent.recommendations topic via Kafka
        try:
            _produce_recommendation(symbol, user_id, signal_strength, settings)
        except Exception as exc:
            logger.warning("Kafka produce failed", error=str(exc))

        # Push via api-gateway internal WS endpoint
        try:
            async with httpx.AsyncClient() as client:
                await client.post(
                    f"{settings.API_GATEWAY_INTERNAL_URL}/api/v1/internal/ws/push",
                    json={
                        "user_id": user_id,
                        "message": {
                            "type": "watchlist.signal",
                            "symbol": symbol,
                            "signal_strength": signal_strength,
                        },
                    },
                    headers={"X-Internal-Secret": settings.INTERNAL_PUSH_SECRET},
                    timeout=3.0,
                )
        except Exception as exc:
            logger.warning("WS push failed", user_id=user_id, symbol=symbol, error=str(exc))

        sent += 1

    return sent


def _produce_recommendation(symbol: str, user_id: str, signal_strength: float, settings) -> None:
    try:
        from confluent_kafka import Producer

        producer = Producer({"bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS})
        producer.produce(
            "agent.recommendations",
            key=user_id,
            value=json.dumps(
                {"symbol": symbol, "user_id": user_id, "signal_strength": signal_strength}
            ).encode(),
        )
        producer.flush(timeout=2)
    except ImportError:
        logger.debug("confluent-kafka not available — skipping produce")
