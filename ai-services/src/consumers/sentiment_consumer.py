from __future__ import annotations

import time
import uuid

import structlog
from confluent_kafka import Consumer, KafkaError, Producer

from src.config import settings
from src.sentiment.finbert_service import FinBERTService
from src.utils.avro_utils import avro_serializer

logger = structlog.get_logger(__name__)

_TOPIC_NEWS_RAW = "news.articles.raw"
_TOPIC_SOCIAL_RAW = "social.posts.raw"
_TOPIC_NEWS_SCORED = "news.articles.scored"
_TOPIC_SOCIAL_SENTIMENT = "social.sentiment"

_SCHEMA_MAP = {
    _TOPIC_NEWS_RAW: "news_article",
    _TOPIC_SOCIAL_RAW: "social_post",
}


class SentimentConsumer:
    """Kafka consumer that runs FinBERT on raw news and social posts.

    news.articles.raw  → FinBERT → news.articles.scored  (NewsSentiment Avro)
    social.posts.raw   → FinBERT → social.sentiment       (SocialSentiment Avro)

    Runs as a background daemon thread inside FastAPI's lifespan.
    """

    def __init__(self, finbert: FinBERTService | None = None) -> None:
        self._finbert = finbert or FinBERTService()
        self._running = False
        self._consumer: Consumer | None = None
        self._producer: Producer | None = None

    def _get_consumer(self) -> Consumer:
        if self._consumer is None:
            cfg = settings.consumer_config(group_id="ai-services.sentiment")
            cfg["enable.auto.commit"] = False
            self._consumer = Consumer(cfg)
            self._consumer.subscribe([_TOPIC_NEWS_RAW, _TOPIC_SOCIAL_RAW])
            logger.info("SentimentConsumer subscribed", topics=[_TOPIC_NEWS_RAW, _TOPIC_SOCIAL_RAW])
        return self._consumer

    def _get_producer(self) -> Producer:
        if self._producer is None:
            self._producer = Producer(settings.producer_config())
        return self._producer

    def _produce(self, topic: str, key: bytes, value: bytes) -> None:
        try:
            self._get_producer().produce(topic=topic, key=key, value=value)
            self._get_producer().poll(0)
        except Exception as exc:
            logger.error("Produce failed", topic=topic, error=str(exc))

    def _process_news(self, record: dict) -> None:
        text = record.get("headline", "") + " " + (record.get("body") or "")
        result = self._finbert.analyze_one(text.strip())
        scored = {
            "id": record["article_id"],
            "symbols": record.get("symbols", []),
            "sentiment_score": result["sentiment_score"],
            "confidence": result["score"],
            "scored_ms": int(time.time() * 1000),
            "source": record.get("source", "finbert"),
        }
        value_bytes = avro_serializer.serialize("news_sentiment", scored)
        self._produce(_TOPIC_NEWS_SCORED, record["article_id"].encode(), value_bytes)

    def _process_social(self, record: dict) -> None:
        text = record.get("title", "") + " " + (record.get("body") or "")
        result = self._finbert.analyze_one(text.strip())
        scored = {
            "id": record["post_id"],
            "symbols": record.get("symbols", []),
            "sentiment_score": result["sentiment_score"],
            "confidence": result["score"],
            "scored_ms": int(time.time() * 1000),
            "source": record.get("platform", "social"),
        }
        value_bytes = avro_serializer.serialize("social_sentiment", scored)
        self._produce(_TOPIC_SOCIAL_SENTIMENT, record["post_id"].encode(), value_bytes)

    def shutdown(self) -> None:
        self._running = False

    def run(self) -> None:
        consumer = self._get_consumer()
        self._running = True
        logger.info("SentimentConsumer running")
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
                    schema = _SCHEMA_MAP[msg.topic()]
                    record = avro_serializer.deserialize(schema, msg.value())
                    if msg.topic() == _TOPIC_NEWS_RAW:
                        self._process_news(record)
                    else:
                        self._process_social(record)
                    consumer.commit(message=msg, asynchronous=False)
                except Exception as exc:
                    logger.error(
                        "Message processing failed",
                        topic=msg.topic(),
                        error=str(exc),
                        exc_info=True,
                    )
        finally:
            if self._producer:
                self._producer.flush(30)
            if self._consumer:
                self._consumer.close()
            logger.info("SentimentConsumer stopped")
