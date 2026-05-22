import signal
import time
from abc import ABC, abstractmethod
from enum import Enum
from typing import Any

import structlog
from confluent_kafka import Consumer, KafkaError

from src.config import settings
from src.sinks.kafka_sink import KafkaSink
from src.utils.avro_utils import StreamAvroSerializer, avro_serializer
from src.utils.monitoring import (
    circuit_breaker_state,
    messages_consumed_total,
    messages_failed_total,
    processing_duration_seconds,
)

logger = structlog.get_logger(__name__)


class CircuitState(Enum):
    CLOSED = 0
    OPEN = 1
    HALF_OPEN = 2


class CircuitBreaker:
    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: float = 60.0,
    ) -> None:
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._last_failure_time: float | None = None

    @property
    def state(self) -> CircuitState:
        if self._state == CircuitState.OPEN:
            if (
                self._last_failure_time
                and (time.time() - self._last_failure_time) >= self.recovery_timeout
            ):
                self._state = CircuitState.HALF_OPEN
        return self._state

    def is_open(self) -> bool:
        return self.state == CircuitState.OPEN

    def record_success(self) -> None:
        self._failure_count = 0
        self._state = CircuitState.CLOSED

    def record_failure(self) -> None:
        self._failure_count += 1
        self._last_failure_time = time.time()
        if self._failure_count >= self.failure_threshold:
            self._state = CircuitState.OPEN


class BaseJob(ABC):
    """Abstract base for all stream-processing jobs.

    Subclasses implement process() and optionally _flush_pending().
    The run() loop: poll → deserialize → process → produce → commit offset.
    """

    def __init__(
        self,
        job_name: str,
        input_topics: list[str],
        output_topics: list[str],
        input_schema: str,
    ) -> None:
        self.job_name = job_name
        self.input_topics = input_topics
        self.output_topics = output_topics
        self.input_schema = input_schema

        self._running = False
        self._consumer: Consumer | None = None
        self._sink = KafkaSink(job_name)
        self._serializer: StreamAvroSerializer = avro_serializer
        self._circuit_breaker = CircuitBreaker()

    # ─── Subclass interface ──────────────────────────────────────

    @abstractmethod
    def process(self, record: dict, raw_msg: Any) -> list[tuple[str, bytes, bytes]]:
        """Process one deserialized record.
        Return list of (topic, key_bytes, value_bytes) to produce.
        """

    def _flush_pending(self, now_ms: int) -> None:
        """Called every poll cycle even when no message arrived. Override for windowed jobs."""

    # ─── Lifecycle ───────────────────────────────────────────────

    def _get_consumer(self) -> Consumer:
        if self._consumer is None:
            group_id = f"{settings.STREAM_PROCESSING_GROUP_PREFIX}.{self.job_name}"
            config = settings.consumer_config(group_id=group_id)
            config["enable.auto.commit"] = False
            self._consumer = Consumer(config)
            logger.info(
                "Consumer created",
                job=self.job_name,
                group_id=group_id,
                topics=self.input_topics,
            )
        return self._consumer

    def shutdown(self) -> None:
        self._running = False

    def run(self) -> None:
        consumer = self._get_consumer()
        consumer.subscribe(self.input_topics)
        self._running = True
        logger.info("Job started", job=self.job_name, topics=self.input_topics)

        try:
            while self._running:
                now_ms = int(time.time() * 1000)
                msg = consumer.poll(timeout=1.0)

                self._flush_pending(now_ms)

                if msg is None:
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    logger.error("Consumer error", job=self.job_name, error=str(msg.error()))
                    self._circuit_breaker.record_failure()
                    circuit_breaker_state.labels(job=self.job_name).set(
                        self._circuit_breaker.state.value
                    )
                    continue

                messages_consumed_total.labels(
                    job=self.job_name, topic=msg.topic()
                ).inc()

                if self._circuit_breaker.is_open():
                    logger.warning("Circuit breaker open — skipping message", job=self.job_name)
                    continue

                start = time.perf_counter()
                try:
                    record = self._serializer.deserialize(self.input_schema, msg.value())
                    outputs = self.process(record, msg)
                    for topic, key_bytes, value_bytes in outputs:
                        self._sink.produce(topic, key_bytes, value_bytes)
                    consumer.commit(message=msg, asynchronous=False)
                    self._circuit_breaker.record_success()
                except Exception as exc:
                    logger.error(
                        "Message processing failed",
                        job=self.job_name,
                        error=str(exc),
                        exc_info=True,
                    )
                    messages_failed_total.labels(
                        job=self.job_name, error_type=type(exc).__name__
                    ).inc()
                    self._circuit_breaker.record_failure()
                finally:
                    processing_duration_seconds.labels(job=self.job_name).observe(
                        time.perf_counter() - start
                    )
                    circuit_breaker_state.labels(job=self.job_name).set(
                        self._circuit_breaker.state.value
                    )
        finally:
            self._sink.flush()
            if self._consumer:
                self._consumer.close()
            logger.info("Job stopped", job=self.job_name)
