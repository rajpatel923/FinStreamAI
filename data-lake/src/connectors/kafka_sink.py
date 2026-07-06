"""Kafka consumer that sinks messages to the Bronze layer."""
from __future__ import annotations

import json
import threading
import time
from typing import Any

import structlog
from confluent_kafka import Consumer, KafkaError, KafkaException

from src.connectors.avro_utils import avro_deserializer
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

_TOPIC_TO_SCHEMA = {
    "market.ticks.raw": "market_tick",
    "news.articles.raw": "news_article",
    "social.posts.raw": "social_post",
    # events.extracted is plain JSON (not Avro) — omit to fall through to json.loads
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
    def _deserialize(topic: str, raw: bytes) -> dict[str, Any] | None:
        """Decode a message per its topic's wire format (Confluent Avro or plain JSON)."""
        try:
            schema_name = _TOPIC_TO_SCHEMA.get(topic)
            if schema_name is not None:
                return avro_deserializer.deserialize(schema_name, raw)
            return json.loads(raw)
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
                raw = msg.value()
                record = self._deserialize(topic, raw)
                if record is None:
                    # Silently skip legacy Confluent Avro wire-format messages on
                    # JSON-only topics (magic byte 0x00 = Confluent wire format).
                    # events.extracted was previously produced as Avro by an older
                    # version of sec_filing_producer; those stale messages in the
                    # topic are unrecoverable and should be discarded quietly.
                    if topic not in _TOPIC_TO_SCHEMA and len(raw) >= 5 and raw[0] == 0x00:
                        logger.debug("Skipping legacy Avro message on JSON topic", topic=topic)
                    else:
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
