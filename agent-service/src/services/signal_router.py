"""SignalRouterConsumer — reads predictions/anomalies, routes to watchers."""
from __future__ import annotations

import asyncio
import json
import threading

import redis
import structlog

logger = structlog.get_logger(__name__)

_TOPICS = ["predictions.signals", "alerts.anomalies"]


class SignalRouterConsumer:
    """Background daemon thread that routes Kafka signals to per-user topics."""

    def __init__(self, bootstrap_servers: str, redis_url: str) -> None:
        self._bootstrap_servers = bootstrap_servers
        self._redis_url = redis_url
        self._running = False
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        self._running = True
        self._thread = threading.Thread(
            target=self._run, name="signal-router", daemon=True
        )
        self._thread.start()

    def stop(self) -> None:
        self._running = False

    def _run(self) -> None:
        try:
            from confluent_kafka import Consumer, KafkaError
        except ImportError:
            logger.warning("confluent-kafka not installed — signal router disabled")
            return

        r = redis.from_url(self._redis_url, decode_responses=True)
        consumer = Consumer(
            {
                "bootstrap.servers": self._bootstrap_servers,
                "group.id": "agent-service-signal-router",
                "auto.offset.reset": "latest",
                "enable.auto.commit": True,
            }
        )
        consumer.subscribe(_TOPICS)
        logger.info("SignalRouterConsumer started", topics=_TOPICS)

        try:
            while self._running:
                msg = consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        logger.error("SignalRouter Kafka error", error=str(msg.error()))
                    continue

                try:
                    value = json.loads(msg.value())
                    symbol = value.get("symbol", "").upper()
                    if not symbol:
                        continue

                    # Find watchers for this symbol
                    watcher_key = f"agent:watchers:{symbol}"
                    watchers = r.smembers(watcher_key)

                    for user_id in watchers:
                        # Produce per-user event to watchlist.signals
                        self._produce_user_signal(symbol, user_id, value)

                except Exception as exc:
                    logger.error("SignalRouter dispatch error", error=str(exc))
        finally:
            consumer.close()
            r.close()

    def _produce_user_signal(self, symbol: str, user_id: str, payload: dict) -> None:
        try:
            from confluent_kafka import Producer
            from src.core.config import settings

            producer = Producer({"bootstrap.servers": self._bootstrap_servers})
            producer.produce(
                "watchlist.signals",
                key=user_id,
                value=json.dumps({**payload, "user_id": user_id}).encode(),
            )
            producer.flush(timeout=1)
        except Exception as exc:
            logger.warning("SignalRouter produce failed", error=str(exc))
