"""Tests for BaseJob circuit breaker and lifecycle."""
import time
from unittest.mock import MagicMock, patch

import pytest

from src.jobs.base_job import CircuitBreaker, CircuitState


# ─── CircuitBreaker ──────────────────────────────────────────────────────────────

class TestCircuitBreaker:
    def test_initial_state_closed(self):
        cb = CircuitBreaker()
        assert cb.state == CircuitState.CLOSED
        assert cb.is_open() is False

    def test_opens_after_threshold(self):
        cb = CircuitBreaker(failure_threshold=3)
        cb.record_failure()
        cb.record_failure()
        assert cb.state == CircuitState.CLOSED
        cb.record_failure()
        assert cb.state == CircuitState.OPEN
        assert cb.is_open() is True

    def test_success_resets_to_closed(self):
        cb = CircuitBreaker(failure_threshold=2)
        cb.record_failure()
        cb.record_failure()
        assert cb.is_open()
        cb.record_success()
        assert cb.state == CircuitState.CLOSED
        assert cb.is_open() is False

    def test_transitions_to_half_open_after_timeout(self):
        cb = CircuitBreaker(failure_threshold=1, recovery_timeout=0.05)
        cb.record_failure()
        assert cb.state == CircuitState.OPEN
        time.sleep(0.1)
        assert cb.state == CircuitState.HALF_OPEN

    def test_failure_count_resets_on_success(self):
        cb = CircuitBreaker(failure_threshold=5)
        cb.record_failure()
        cb.record_failure()
        cb.record_success()
        assert cb._failure_count == 0

    def test_multiple_opens_stay_open(self):
        cb = CircuitBreaker(failure_threshold=1)
        cb.record_failure()
        cb.record_failure()  # already open, stays open
        assert cb.state == CircuitState.OPEN


# ─── BaseJob lifecycle ───────────────────────────────────────────────────────────

class TestBaseJobLifecycle:
    def _make_job(self):
        from src.jobs.base_job import BaseJob

        class ConcreteJob(BaseJob):
            def __init__(self):
                super().__init__(
                    job_name="test_job",
                    input_topics=["input.topic"],
                    output_topics=["output.topic"],
                    input_schema="market_tick",
                )
                self.processed = []

            def process(self, record, raw_msg):
                self.processed.append(record)
                return []

        return ConcreteJob()

    def test_shutdown_sets_running_false(self):
        job = self._make_job()
        job._running = True
        job.shutdown()
        assert job._running is False

    def test_consumer_created_with_correct_group_id(self):
        from src.config import settings

        job = self._make_job()
        with patch("src.jobs.base_job.Consumer") as mock_consumer_cls:
            mock_consumer_cls.return_value = MagicMock()
            consumer = job._get_consumer()
            call_args = mock_consumer_cls.call_args[0][0]
            expected_group = f"{settings.STREAM_PROCESSING_GROUP_PREFIX}.test_job"
            assert call_args["group.id"] == expected_group
            assert call_args["enable.auto.commit"] is False

    def test_consumer_cached(self):
        job = self._make_job()
        with patch("src.jobs.base_job.Consumer") as mock_consumer_cls:
            mock_consumer_cls.return_value = MagicMock()
            c1 = job._get_consumer()
            c2 = job._get_consumer()
            assert c1 is c2
            mock_consumer_cls.assert_called_once()

    def test_run_stops_on_shutdown(self, mock_kafka_producer):
        job = self._make_job()
        call_count = 0

        def fake_poll(timeout):
            nonlocal call_count
            call_count += 1
            if call_count >= 2:
                job._running = False  # simulate shutdown after 2 polls
            return None  # no messages

        with patch("src.jobs.base_job.Consumer") as mock_cls:
            mock_consumer = MagicMock()
            mock_consumer.poll.side_effect = fake_poll
            mock_cls.return_value = mock_consumer
            job.run()

        assert call_count >= 2

    def test_run_commits_offset_after_process(self, mock_kafka_producer):
        from tests.conftest import avro_encode

        job = self._make_job()
        tick = {
            "event_id": "abc", "symbol": "AAPL",
            "timestamp_ms": int(time.time() * 1000),
            "price": 100.0, "volume": 100,
            "bid_price": None, "ask_price": None, "bid_size": None, "ask_size": None,
            "exchange": None, "source": "test", "is_mock": False,
        }
        msg_bytes = avro_encode("market_tick", tick)
        call_count = 0

        def fake_poll(timeout):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                msg = MagicMock()
                msg.value.return_value = msg_bytes
                msg.topic.return_value = "input.topic"
                msg.error.return_value = None
                return msg
            job._running = False
            return None

        with patch("src.jobs.base_job.Consumer") as mock_cls:
            mock_consumer = MagicMock()
            mock_consumer.poll.side_effect = fake_poll
            mock_cls.return_value = mock_consumer
            job.run()

        mock_consumer.commit.assert_called_once()
        assert len(job.processed) == 1

    def test_circuit_breaker_blocks_processing_when_open(self, mock_kafka_producer):
        from tests.conftest import avro_encode

        job = self._make_job()
        # Force circuit breaker open
        for _ in range(6):
            job._circuit_breaker.record_failure()
        assert job._circuit_breaker.is_open()

        tick = {
            "event_id": "abc", "symbol": "AAPL",
            "timestamp_ms": int(time.time() * 1000),
            "price": 100.0, "volume": 100,
            "bid_price": None, "ask_price": None, "bid_size": None, "ask_size": None,
            "exchange": None, "source": "test", "is_mock": False,
        }
        msg_bytes = avro_encode("market_tick", tick)
        call_count = 0

        def fake_poll(timeout):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                msg = MagicMock()
                msg.value.return_value = msg_bytes
                msg.topic.return_value = "input.topic"
                msg.error.return_value = None
                return msg
            job._running = False
            return None

        with patch("src.jobs.base_job.Consumer") as mock_cls:
            mock_consumer = MagicMock()
            mock_consumer.poll.side_effect = fake_poll
            mock_cls.return_value = mock_consumer
            job.run()

        # Message should not have been processed
        assert len(job.processed) == 0
