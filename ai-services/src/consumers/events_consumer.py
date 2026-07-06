from __future__ import annotations

import json
import time
import uuid

import structlog
from confluent_kafka import Consumer, KafkaError, Producer

from src.config import settings
from src.events.claude_extractor import ClaudeEventExtractor
from src.utils.avro_utils import avro_serializer

logger = structlog.get_logger(__name__)

_TOPIC_INPUT = "news.articles.raw"
_TOPIC_OUTPUT = "events.extracted"


class EventsConsumer:
    """Kafka consumer that extracts financial events via Claude API.

    news.articles.raw → Claude (or spaCy fallback) → events.extracted (JSON)

    Runs as a background daemon thread inside FastAPI's lifespan.
    """

    def __init__(self, extractor: ClaudeEventExtractor | None = None) -> None:
        self._extractor = extractor or ClaudeEventExtractor()
        self._running = False
        self._consumer: Consumer | None = None
        self._producer: Producer | None = None

    def _get_consumer(self) -> Consumer:
        if self._consumer is None:
            cfg = settings.consumer_config(group_id="ai-services.events")
            cfg["enable.auto.commit"] = False
            self._consumer = Consumer(cfg)
            self._consumer.subscribe([_TOPIC_INPUT])
            logger.info("EventsConsumer subscribed", topic=_TOPIC_INPUT)
        return self._consumer

    def _get_producer(self) -> Producer:
        if self._producer is None:
            self._producer = Producer(settings.producer_config())
        return self._producer

    def _process(self, record: dict) -> None:
        text = record.get("headline", "") + " " + (record.get("body") or "")
        event = self._extractor.extract(text.strip(), source_id=record.get("article_id"))
        event["symbols"] = record.get("symbols", [])
        # Normalize to fields expected by data-lake quality checker
        event.setdefault("event_id", str(uuid.uuid4()))
        event.setdefault("timestamp", event.get("extracted_ms", int(time.time() * 1000)))
        value_bytes = json.dumps(event).encode()
        key = (record.get("article_id") or "unknown").encode()
        try:
            self._get_producer().produce(topic=_TOPIC_OUTPUT, key=key, value=value_bytes)
            self._get_producer().poll(0)
        except Exception as exc:
            logger.error("Event produce failed", error=str(exc))

    def shutdown(self) -> None:
        self._running = False

    def run(self) -> None:
        consumer = self._get_consumer()
        self._running = True
        logger.info("EventsConsumer running")
        try:
            while self._running:
                msg = consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        logger.error("Kafka error", error=str(msg.error()))
                    continue
                try:
                    record = avro_serializer.deserialize("news_article", msg.value())
                    self._process(record)
                    consumer.commit(message=msg, asynchronous=False)
                except Exception as exc:
                    logger.error(
                        "Event processing failed",
                        error=str(exc),
                        exc_info=True,
                    )
        finally:
            if self._producer:
                self._producer.flush(30)
            if self._consumer:
                self._consumer.close()
            logger.info("EventsConsumer stopped")
