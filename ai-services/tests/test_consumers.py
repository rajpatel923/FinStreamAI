"""Unit tests for Kafka consumer message-processing logic.

Kafka poll loops, Producer, and Consumer are all mocked.
Only the _process_* methods (pure transformation logic) are exercised.
"""
from __future__ import annotations

import time
from unittest.mock import MagicMock, patch

import pytest

from src.consumers.sentiment_consumer import SentimentConsumer
from src.consumers.events_consumer import EventsConsumer


# ─── SentimentConsumer ───────────────────────────────────────────────────────

class TestSentimentConsumerProcessing:
    def _consumer(self):
        mock_finbert = MagicMock()
        mock_finbert.analyze_one.return_value = {
            "label": "positive", "score": 0.95, "sentiment_score": 1.0
        }
        consumer = SentimentConsumer(finbert=mock_finbert)
        # Replace producer with mock so no real Kafka needed
        mock_producer = MagicMock()
        consumer._producer = mock_producer
        return consumer, mock_finbert, mock_producer

    def test_process_news_calls_finbert(self):
        consumer, mock_finbert, _ = self._consumer()
        record = {
            "article_id": "art-1",
            "headline": "Apple beats earnings",
            "body": "Strong Q1 results.",
            "symbols": ["AAPL"],
            "source": "reuters",
        }
        with patch.object(consumer, "_produce") as mock_produce, \
             patch("src.consumers.sentiment_consumer.avro_serializer") as mock_avro:
            mock_avro.serialize.return_value = b"\x00\x01\x02"
            consumer._process_news(record)

        mock_finbert.analyze_one.assert_called_once()
        call_text = mock_finbert.analyze_one.call_args[0][0]
        assert "Apple beats earnings" in call_text

    def test_process_news_produces_to_scored_topic(self):
        consumer, _, _ = self._consumer()
        record = {
            "article_id": "art-2",
            "headline": "Headline",
            "body": None,
            "symbols": ["MSFT"],
            "source": "test",
        }
        produced = []

        def capture_produce(topic, key, value):
            produced.append((topic, key, value))

        with patch.object(consumer, "_produce", side_effect=capture_produce), \
             patch("src.consumers.sentiment_consumer.avro_serializer") as mock_avro:
            mock_avro.serialize.return_value = b"\x00" * 10
            consumer._process_news(record)

        assert len(produced) == 1
        assert produced[0][0] == "news.articles.scored"

    def test_process_news_key_is_article_id(self):
        consumer, _, _ = self._consumer()
        record = {
            "article_id": "unique-id-99",
            "headline": "h",
            "body": None,
            "symbols": [],
            "source": "s",
        }
        keys = []
        with patch.object(consumer, "_produce", side_effect=lambda t, k, v: keys.append(k)), \
             patch("src.consumers.sentiment_consumer.avro_serializer") as mock_avro:
            mock_avro.serialize.return_value = b"\x00"
            consumer._process_news(record)

        assert keys[0] == b"unique-id-99"

    def test_process_social_calls_finbert(self):
        consumer, mock_finbert, _ = self._consumer()
        record = {
            "post_id": "post-1",
            "title": "Tesla short squeeze incoming",
            "body": "WSB is all in.",
            "symbols": ["TSLA"],
            "platform": "reddit",
        }
        with patch.object(consumer, "_produce"), \
             patch("src.consumers.sentiment_consumer.avro_serializer") as mock_avro:
            mock_avro.serialize.return_value = b"\x00"
            consumer._process_social(record)

        mock_finbert.analyze_one.assert_called_once()

    def test_process_social_produces_to_sentiment_topic(self):
        consumer, _, _ = self._consumer()
        record = {
            "post_id": "post-2",
            "title": "T",
            "body": None,
            "symbols": [],
            "platform": "twitter",
        }
        produced_topics = []
        with patch.object(consumer, "_produce", side_effect=lambda t, k, v: produced_topics.append(t)), \
             patch("src.consumers.sentiment_consumer.avro_serializer") as mock_avro:
            mock_avro.serialize.return_value = b"\x00"
            consumer._process_social(record)

        assert produced_topics[0] == "social.sentiment"

    def test_shutdown_sets_running_false(self):
        consumer = SentimentConsumer()
        consumer._running = True
        consumer.shutdown()
        assert consumer._running is False


# ─── EventsConsumer ──────────────────────────────────────────────────────────

class TestEventsConsumerProcessing:
    def _consumer(self):
        mock_extractor = MagicMock()
        mock_extractor.extract.return_value = {
            "event_type": "earnings",
            "companies": ["AAPL"],
            "date": "2024-01-15",
            "confidence": 0.9,
            "summary": "Strong Q1.",
            "source_id": "art-1",
            "extracted_ms": int(time.time() * 1000),
        }
        consumer = EventsConsumer(extractor=mock_extractor)
        return consumer, mock_extractor

    def test_process_calls_extractor(self):
        consumer, mock_extractor = self._consumer()
        record = {
            "article_id": "art-1",
            "headline": "Apple earnings beat",
            "body": "Strong Q1.",
            "symbols": ["AAPL"],
        }
        with patch.object(consumer, "_get_producer") as mock_get_prod:
            mock_producer = MagicMock()
            mock_get_prod.return_value = mock_producer
            consumer._process(record)

        mock_extractor.extract.assert_called_once()
        call_text = mock_extractor.extract.call_args[0][0]
        assert "Apple earnings beat" in call_text

    def test_process_attaches_symbols(self):
        consumer, mock_extractor = self._consumer()
        record = {
            "article_id": "art-2",
            "headline": "MSFT acquires company",
            "body": None,
            "symbols": ["MSFT", "GOOGL"],
        }
        produced_values = []

        with patch.object(consumer, "_get_producer") as mock_get_prod:
            mock_producer = MagicMock()

            def capture_produce(topic, key, value):
                import json
                produced_values.append(json.loads(value))

            mock_producer.produce.side_effect = capture_produce
            mock_get_prod.return_value = mock_producer
            consumer._process(record)

        assert len(produced_values) == 1
        assert produced_values[0]["symbols"] == ["MSFT", "GOOGL"]

    def test_process_produces_to_events_topic(self):
        consumer, _ = self._consumer()
        record = {
            "article_id": "art-3",
            "headline": "News",
            "body": None,
            "symbols": [],
        }
        produced_topics = []
        with patch.object(consumer, "_get_producer") as mock_get_prod:
            mock_producer = MagicMock()

            def capture(topic, key, value):
                produced_topics.append(topic)

            mock_producer.produce.side_effect = capture
            mock_get_prod.return_value = mock_producer
            consumer._process(record)

        assert produced_topics[0] == "events.extracted"

    def test_shutdown_sets_running_false(self):
        consumer = EventsConsumer()
        consumer._running = True
        consumer.shutdown()
        assert consumer._running is False


# ─── Consumer infrastructure (get_consumer, get_producer, _produce) ─────────

class TestSentimentConsumerInfrastructure:
    def test_get_consumer_creates_confluent_consumer(self):
        consumer = SentimentConsumer()
        with patch("src.consumers.sentiment_consumer.Consumer") as mock_cls:
            mock_instance = MagicMock()
            mock_cls.return_value = mock_instance
            c = consumer._get_consumer()
        assert c is mock_instance
        mock_instance.subscribe.assert_called_once()

    def test_get_consumer_cached(self):
        consumer = SentimentConsumer()
        with patch("src.consumers.sentiment_consumer.Consumer") as mock_cls:
            mock_instance = MagicMock()
            mock_cls.return_value = mock_instance
            c1 = consumer._get_consumer()
            c2 = consumer._get_consumer()
        assert c1 is c2
        mock_cls.assert_called_once()

    def test_get_producer_creates_confluent_producer(self):
        consumer = SentimentConsumer()
        with patch("src.consumers.sentiment_consumer.Producer") as mock_cls:
            mock_instance = MagicMock()
            mock_cls.return_value = mock_instance
            p = consumer._get_producer()
        assert p is mock_instance

    def test_get_producer_cached(self):
        consumer = SentimentConsumer()
        with patch("src.consumers.sentiment_consumer.Producer") as mock_cls:
            mock_instance = MagicMock()
            mock_cls.return_value = mock_instance
            p1 = consumer._get_producer()
            p2 = consumer._get_producer()
        assert p1 is p2
        mock_cls.assert_called_once()

    def test_produce_calls_producer_produce(self):
        consumer = SentimentConsumer()
        mock_producer = MagicMock()
        consumer._producer = mock_producer
        consumer._produce("test.topic", b"key", b"value")
        mock_producer.produce.assert_called_once_with(
            topic="test.topic", key=b"key", value=b"value"
        )

    def test_produce_handles_exception_gracefully(self):
        consumer = SentimentConsumer()
        mock_producer = MagicMock()
        mock_producer.produce.side_effect = Exception("broker unavailable")
        consumer._producer = mock_producer
        # Should not raise
        consumer._produce("test.topic", b"key", b"value")

    def test_run_stops_immediately_on_shutdown(self):
        consumer = SentimentConsumer()
        with patch("src.consumers.sentiment_consumer.Consumer") as mock_cls, \
             patch("src.consumers.sentiment_consumer.Producer"):
            mock_kafka = MagicMock()
            call_count = 0

            def fake_poll(timeout):
                nonlocal call_count
                call_count += 1
                consumer.shutdown()
                return None

            mock_kafka.poll.side_effect = fake_poll
            mock_cls.return_value = mock_kafka
            consumer.run()

        assert call_count >= 1
        assert not consumer._running


class TestEventsConsumerInfrastructure:
    def test_get_consumer_creates_confluent_consumer(self):
        consumer = EventsConsumer()
        with patch("src.consumers.events_consumer.Consumer") as mock_cls:
            mock_instance = MagicMock()
            mock_cls.return_value = mock_instance
            c = consumer._get_consumer()
        assert c is mock_instance
        mock_instance.subscribe.assert_called_once()

    def test_get_consumer_cached(self):
        consumer = EventsConsumer()
        with patch("src.consumers.events_consumer.Consumer") as mock_cls:
            mock_instance = MagicMock()
            mock_cls.return_value = mock_instance
            c1 = consumer._get_consumer()
            c2 = consumer._get_consumer()
        assert c1 is c2
        mock_cls.assert_called_once()

    def test_get_producer_creates_confluent_producer(self):
        consumer = EventsConsumer()
        with patch("src.consumers.events_consumer.Producer") as mock_cls:
            mock_instance = MagicMock()
            mock_cls.return_value = mock_instance
            p = consumer._get_producer()
        assert p is mock_instance

    def test_run_stops_immediately_on_shutdown(self):
        consumer = EventsConsumer()
        with patch("src.consumers.events_consumer.Consumer") as mock_cls, \
             patch("src.consumers.events_consumer.Producer"):
            mock_kafka = MagicMock()
            call_count = 0

            def fake_poll(timeout):
                nonlocal call_count
                call_count += 1
                consumer.shutdown()
                return None

            mock_kafka.poll.side_effect = fake_poll
            mock_cls.return_value = mock_kafka
            consumer.run()

        assert call_count >= 1
        assert not consumer._running
