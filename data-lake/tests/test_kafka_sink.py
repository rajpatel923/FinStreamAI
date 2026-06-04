"""Tests for KafkaSinkConsumer."""
from __future__ import annotations

import threading
import time
from unittest.mock import MagicMock, patch, PropertyMock

import pytest
from src.connectors.kafka_sink import KafkaSinkConsumer, _TOPIC_TO_WRITER


@pytest.fixture
def bronze_mock():
    b = MagicMock()
    b.write_market_tick = MagicMock()
    b.write_news_article = MagicMock()
    b.write_social_post = MagicMock()
    b.write_event = MagicMock()
    return b


@pytest.fixture
def consumer_config():
    return {
        "bootstrap.servers": "localhost:9092",
        "group.id": "test-sink",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    }


class TestKafkaSinkConsumer:
    def test_topic_to_writer_mapping(self):
        assert "market.ticks.raw" in _TOPIC_TO_WRITER
        assert "news.articles.raw" in _TOPIC_TO_WRITER
        assert "social.posts.raw" in _TOPIC_TO_WRITER
        assert "events.extracted" in _TOPIC_TO_WRITER

    def test_shutdown_sets_stop_event(self, consumer_config, bronze_mock):
        sink = KafkaSinkConsumer(consumer_config, bronze_mock)
        assert not sink._stop_event.is_set()
        sink.shutdown()
        assert sink._stop_event.is_set()

    def test_deserialize_invalid_returns_none(self, consumer_config, bronze_mock):
        sink = KafkaSinkConsumer(consumer_config, bronze_mock)
        result = sink._deserialize(b"not_valid_avro_data_xyz")
        assert result is None

    def test_deserialize_empty_bytes(self, consumer_config, bronze_mock):
        sink = KafkaSinkConsumer(consumer_config, bronze_mock)
        assert sink._deserialize(b"") is None

    def test_run_stops_on_shutdown(self, consumer_config, bronze_mock):
        sink = KafkaSinkConsumer(consumer_config, bronze_mock)

        mock_consumer = MagicMock()
        mock_consumer.poll.return_value = None

        with patch("src.connectors.kafka_sink.Consumer", return_value=mock_consumer):
            sink.shutdown()  # pre-set stop event
            sink.run()  # should exit immediately

        mock_consumer.close.assert_called_once()

    def test_run_calls_correct_writer(self, consumer_config, bronze_mock):
        sink = KafkaSinkConsumer(consumer_config, bronze_mock)

        msg = MagicMock()
        msg.error.return_value = None
        msg.topic.return_value = "market.ticks.raw"
        msg.value.return_value = b""

        call_count = [0]

        def fake_poll(timeout):
            call_count[0] += 1
            if call_count[0] == 1:
                return msg
            sink.shutdown()
            return None

        mock_consumer = MagicMock()
        mock_consumer.poll.side_effect = fake_poll

        with patch("src.connectors.kafka_sink.Consumer", return_value=mock_consumer):
            with patch.object(sink, "_deserialize", return_value={"symbol": "AAPL", "price": 150.0, "timestamp": 1700000000}):
                sink.run()

        bronze_mock.write_market_tick.assert_called_once()

    def test_checkpoint_triggers_commit(self, consumer_config, bronze_mock):
        sink = KafkaSinkConsumer(consumer_config, bronze_mock)
        sink._consumer = MagicMock()
        # Force last_checkpoint to far in the past
        sink._last_checkpoint = 0
        sink._maybe_checkpoint()
        sink._consumer.commit.assert_called_once_with(asynchronous=True)
