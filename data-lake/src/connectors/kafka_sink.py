"""Kafka consumer that sinks messages to the Bronze layer."""
from __future__ import annotations

import threading
import time
from typing import Any

import fastavro
import io
import structlog
from confluent_kafka import Consumer, KafkaError, KafkaException

from src.lake.bronze_layer import BronzeLayer

logger = structlog.get_logger(__name__)

_TOPICS = [
    "market.ticks.raw",
    "news.articles.raw",
    "social.posts.raw",
    "events.extracted",
]

_TOPIC_TO_WRITER = {
    "market.ticks.raw": "write_market_tick",
    "news.articles.raw": "write_news_article",
    "social.posts.raw": "write_social_post",
    "events.extracted": "write_event",
}

_CHECKPOINT_INTERVAL_S = 300  # 5 minutes


class KafkaSinkConsumer:
    """Consumes Kafka topics and writes records to Bronze Delta tables."""

    def __init__(
        self,
        consumer_config: dict[str, Any],
        bronze_layer: BronzeLayer,
    ) -> None:
        self._config = consumer_config
        self._bronze = bronze_layer
        self._stop_event = threading.Event()
        self._consumer: Consumer | None = None
        self._last_checkpoint = time.monotonic()

    # ------------------------------------------------------------------
    # Avro deserialization
    # ------------------------------------------------------------------
    @staticmethod
    def _deserialize(raw: bytes) -> dict[str, Any] | None:
        """Attempt Avro deserialization; fall back to plain dict on failure."""
        try:
            # Confluent wire format: 1 magic byte + 4-byte schema id + avro payload
            if raw and len(raw) > 5 and raw[0] == 0:
                payload = io.BytesIO(raw[5:])
            else:
                payload = io.BytesIO(raw)
            reader = fastavro.reader(payload)
            records = list(reader)
            return records[0] if records else None
        except Exception:
            # If deserialization fails return None (will be quarantined upstream)
            return None

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------
    def run(self) -> None:
        self._consumer = Consumer(self._config)
        self._consumer.subscribe(_TOPICS)
        logger.info("KafkaSinkConsumer started", topics=_TOPICS)

        try:
            while not self._stop_event.is_set():
                msg = self._consumer.poll(timeout=1.0)
                if msg is None:
                    self._maybe_checkpoint()
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    raise KafkaException(msg.error())

                topic = msg.topic()
                record = self._deserialize(msg.value())
                if record is None:
                    logger.warning("Failed to deserialize message", topic=topic)
                    continue

                writer_name = _TOPIC_TO_WRITER.get(topic)
                if writer_name:
                    writer = getattr(self._bronze, writer_name)
                    try:
                        writer(record)
                    except Exception as exc:
                        logger.error("Bronze write failed", topic=topic, error=str(exc))

                self._maybe_checkpoint()
        finally:
            if self._consumer:
                self._consumer.close()
            logger.info("KafkaSinkConsumer stopped")

    def _maybe_checkpoint(self) -> None:
        now = time.monotonic()
        if now - self._last_checkpoint >= _CHECKPOINT_INTERVAL_S:
            if self._consumer:
                self._consumer.commit(asynchronous=True)
            self._last_checkpoint = now
            logger.debug("Kafka offset checkpoint committed")

    def shutdown(self) -> None:
        self._stop_event.set()
