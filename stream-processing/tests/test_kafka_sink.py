"""Tests for KafkaSink."""
from unittest.mock import MagicMock, call, patch

import pytest
from confluent_kafka import KafkaException


class TestKafkaSinkLazyProducer:
    def test_no_producer_on_init(self):
        with patch("src.sinks.kafka_sink.Producer"):
            from src.sinks.kafka_sink import KafkaSink
            sink = KafkaSink("test-job")
            assert sink._producer is None

    def test_producer_created_on_first_produce(self):
        with patch("src.sinks.kafka_sink.Producer") as mock_prod_cls:
            mock_prod = MagicMock()
            mock_prod_cls.return_value = mock_prod
            from src.sinks.kafka_sink import KafkaSink
            sink = KafkaSink("test-job")
            sink.produce("test.topic", b"key", b"value")
            mock_prod_cls.assert_called_once()

    def test_producer_cached_across_calls(self):
        with patch("src.sinks.kafka_sink.Producer") as mock_prod_cls:
            mock_prod = MagicMock()
            mock_prod_cls.return_value = mock_prod
            from src.sinks.kafka_sink import KafkaSink
            sink = KafkaSink("test-job")
            sink.produce("t", b"k", b"v")
            sink.produce("t", b"k", b"v")
            mock_prod_cls.assert_called_once()


class TestKafkaSinkProduce:
    def _make_sink_with_mock(self):
        with patch("src.sinks.kafka_sink.Producer") as mock_prod_cls:
            mock_prod = MagicMock()
            mock_prod_cls.return_value = mock_prod
            from src.sinks.kafka_sink import KafkaSink
            sink = KafkaSink("test-job")
            sink._producer = mock_prod
            return sink, mock_prod

    def test_produce_calls_producer_with_correct_args(self):
        sink, mock_prod = self._make_sink_with_mock()
        sink.produce("my.topic", b"mykey", b"myvalue")
        mock_prod.produce.assert_called_once_with(
            topic="my.topic",
            key=b"mykey",
            value=b"myvalue",
            on_delivery=sink._on_delivery,
        )

    def test_produce_calls_poll_zero(self):
        sink, mock_prod = self._make_sink_with_mock()
        sink.produce("t", b"k", b"v")
        mock_prod.poll.assert_called_with(0)

    def test_produce_catches_kafka_exception(self):
        sink, mock_prod = self._make_sink_with_mock()
        mock_prod.produce.side_effect = KafkaException("broker unavailable")
        sink.produce("t", b"k", b"v")  # must not raise

    def test_produce_increments_failed_counter_on_kafka_exception(self):
        from unittest.mock import patch as mpatch
        sink, mock_prod = self._make_sink_with_mock()
        mock_prod.produce.side_effect = KafkaException("error")
        with mpatch("src.sinks.kafka_sink.messages_failed_total") as mock_counter:
            mock_labels = MagicMock()
            mock_counter.labels.return_value = mock_labels
            sink.produce("t", b"k", b"v")
            mock_counter.labels.assert_called_once()
            mock_labels.inc.assert_called_once()

    def test_produce_with_none_key(self):
        sink, mock_prod = self._make_sink_with_mock()
        sink.produce("t", None, b"v")
        mock_prod.produce.assert_called_once_with(
            topic="t", key=None, value=b"v", on_delivery=sink._on_delivery
        )


class TestKafkaSinkDeliveryCallback:
    def _make_sink(self):
        with patch("src.sinks.kafka_sink.Producer"):
            from src.sinks.kafka_sink import KafkaSink
            sink = KafkaSink("test-job")
            return sink

    def test_on_delivery_success_increments_produced_counter(self):
        from unittest.mock import patch as mpatch
        sink = self._make_sink()
        mock_msg = MagicMock()
        mock_msg.topic.return_value = "my.topic"
        mock_msg.value.return_value = b"hello"
        with mpatch("src.sinks.kafka_sink.messages_produced_total") as mock_counter:
            mock_labels = MagicMock()
            mock_counter.labels.return_value = mock_labels
            sink._on_delivery(None, mock_msg)
            mock_counter.labels.assert_called_with(job="test-job", topic="my.topic")
            mock_labels.inc.assert_called_once()

    def test_on_delivery_success_increments_bytes_counter(self):
        from unittest.mock import patch as mpatch
        sink = self._make_sink()
        mock_msg = MagicMock()
        mock_msg.topic.return_value = "t"
        mock_msg.value.return_value = b"abcde"
        with mpatch("src.sinks.kafka_sink.bytes_produced_total") as mock_counter:
            mock_labels = MagicMock()
            mock_counter.labels.return_value = mock_labels
            sink._on_delivery(None, mock_msg)
            mock_labels.inc.assert_called_once_with(5)

    def test_on_delivery_error_increments_failed_counter(self):
        from unittest.mock import patch as mpatch
        sink = self._make_sink()
        mock_err = MagicMock()
        mock_msg = MagicMock()
        with mpatch("src.sinks.kafka_sink.messages_failed_total") as mock_counter:
            mock_labels = MagicMock()
            mock_counter.labels.return_value = mock_labels
            sink._on_delivery(mock_err, mock_msg)
            mock_counter.labels.assert_called_with(
                job="test-job", error_type="delivery_error"
            )
            mock_labels.inc.assert_called_once()

    def test_on_delivery_success_with_empty_value(self):
        from unittest.mock import patch as mpatch
        sink = self._make_sink()
        mock_msg = MagicMock()
        mock_msg.topic.return_value = "t"
        mock_msg.value.return_value = None  # value can be None
        with mpatch("src.sinks.kafka_sink.bytes_produced_total") as mock_counter:
            mock_labels = MagicMock()
            mock_counter.labels.return_value = mock_labels
            sink._on_delivery(None, mock_msg)
            mock_labels.inc.assert_called_once_with(0)


class TestKafkaSinkFlushClose:
    def test_flush_calls_producer_flush(self):
        with patch("src.sinks.kafka_sink.Producer") as mock_prod_cls:
            mock_prod = MagicMock()
            mock_prod_cls.return_value = mock_prod
            from src.sinks.kafka_sink import KafkaSink
            sink = KafkaSink("test-job")
            sink._producer = mock_prod
            sink.flush(timeout=15.0)
            mock_prod.flush.assert_called_once_with(timeout=15.0)

    def test_flush_noop_when_no_producer(self):
        with patch("src.sinks.kafka_sink.Producer"):
            from src.sinks.kafka_sink import KafkaSink
            sink = KafkaSink("test-job")
            sink.flush()  # _producer is None, must not raise

    def test_close_flushes_then_nulls_producer(self):
        with patch("src.sinks.kafka_sink.Producer") as mock_prod_cls:
            mock_prod = MagicMock()
            mock_prod_cls.return_value = mock_prod
            from src.sinks.kafka_sink import KafkaSink
            sink = KafkaSink("test-job")
            sink._producer = mock_prod
            sink.close()
            mock_prod.flush.assert_called_once()
            assert sink._producer is None

    def test_multiple_produce_before_flush(self):
        with patch("src.sinks.kafka_sink.Producer") as mock_prod_cls:
            mock_prod = MagicMock()
            mock_prod_cls.return_value = mock_prod
            from src.sinks.kafka_sink import KafkaSink
            sink = KafkaSink("test-job")
            sink._producer = mock_prod
            sink.produce("t", b"k1", b"v1")
            sink.produce("t", b"k2", b"v2")
            sink.produce("t", b"k3", b"v3")
            assert mock_prod.produce.call_count == 3
            sink.flush()
            mock_prod.flush.assert_called_once()
