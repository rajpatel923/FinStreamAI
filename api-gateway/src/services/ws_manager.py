"""WebSocket ConnectionManager + Kafka bridge consumer."""
from __future__ import annotations

import asyncio
import json
import threading
from collections import defaultdict
from typing import Any

import redis.asyncio as aioredis
import structlog
from fastapi import WebSocket

logger = structlog.get_logger(__name__)

_PING_INTERVAL = 30
_PONG_TIMEOUT = 10


class ConnectionManager:
    def __init__(self) -> None:
        # user_id -> list[WebSocket]
        self._connections: dict[str, list[WebSocket]] = defaultdict(list)
        self._lock = asyncio.Lock()

    async def connect(self, user_id: str, ws: WebSocket) -> None:
        await ws.accept()
        async with self._lock:
            self._connections[user_id].append(ws)
        logger.info("WebSocket connected", user_id=user_id)

    async def disconnect(self, user_id: str, ws: WebSocket) -> None:
        async with self._lock:
            conns = self._connections.get(user_id, [])
            if ws in conns:
                conns.remove(ws)
            if not conns:
                self._connections.pop(user_id, None)
        logger.info("WebSocket disconnected", user_id=user_id)

    async def broadcast(self, user_id: str, message: dict[str, Any]) -> None:
        """Send a message to all connections for a user."""
        conns = list(self._connections.get(user_id, []))
        dead: list[WebSocket] = []
        payload = json.dumps(message, default=str)
        for ws in conns:
            try:
                await ws.send_text(payload)
            except Exception:
                dead.append(ws)
        for ws in dead:
            await self.disconnect(user_id, ws)

    async def broadcast_all(self, message: dict[str, Any]) -> None:
        """Broadcast to all connected users (e.g. global alerts)."""
        all_users = list(self._connections.keys())
        for user_id in all_users:
            await self.broadcast(user_id, message)

    def active_user_ids(self) -> list[str]:
        return list(self._connections.keys())


class KafkaBridgeConsumer:
    """Background thread that reads from Kafka and pushes to ConnectionManager."""

    TOPICS = [
        "market.ticks.clean",
        "news.articles.scored",
        "events.extracted",
        "alerts.anomalies",
        "predictions.signals",
        "agent.recommendations",
        "watchlist.signals",
    ]

    def __init__(
        self,
        manager: ConnectionManager,
        bootstrap_servers: str,
        redis_url: str,
    ) -> None:
        self._manager = manager
        self._bootstrap_servers = bootstrap_servers
        self._redis_url = redis_url
        self._running = False
        self._thread: threading.Thread | None = None
        self._loop: asyncio.AbstractEventLoop | None = None

    def start(self, loop: asyncio.AbstractEventLoop) -> None:
        self._loop = loop
        self._running = True
        self._thread = threading.Thread(target=self._run, name="kafka-ws-bridge", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._running = False

    def _run(self) -> None:
        try:
            from confluent_kafka import Consumer, KafkaError
        except ImportError:
            logger.warning("confluent-kafka not installed — WS bridge disabled")
            return

        consumer = Consumer(
            {
                "bootstrap.servers": self._bootstrap_servers,
                "group.id": "api-gateway-ws",
                "auto.offset.reset": "latest",
                "enable.auto.commit": True,
            }
        )
        consumer.subscribe(self.TOPICS)
        logger.info("Kafka WS bridge started", topics=self.TOPICS)

        while self._running:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    logger.error("Kafka WS bridge error", error=str(msg.error()))
                continue
            try:
                value = json.loads(msg.value())
                symbol = value.get("symbol")
                topic = msg.topic()
                payload = {"type": topic, "data": value}

                if self._loop and not self._loop.is_closed():
                    # Push to subscribed users
                    asyncio.run_coroutine_threadsafe(
                        self._dispatch(symbol, payload), self._loop
                    )
            except Exception as exc:
                logger.error("WS bridge dispatch error", error=str(exc))

        consumer.close()

    async def _dispatch(self, symbol: str | None, payload: dict) -> None:
        """Dispatch to users who subscribed to this symbol."""
        active = self._manager.active_user_ids()
        # Build a simple Redis-based subscription lookup
        try:
            redis = aioredis.from_url(self._redis_url, decode_responses=True)
            for user_id in active:
                subs_raw = await redis.smembers(f"ws:subs:{user_id}:symbols")
                if symbol is None or not subs_raw or symbol in subs_raw or "*" in subs_raw:
                    await self._manager.broadcast(user_id, payload)
            await redis.aclose()
        except Exception as exc:
            logger.error("WS dispatch Redis error", error=str(exc))
            # Fallback: broadcast to all
            await self._manager.broadcast_all(payload)
