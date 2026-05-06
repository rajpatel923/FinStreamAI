import signal
import time
from abc import ABC, abstractmethod
from enum import Enum
from typing import Any

import structlog
from confluent_kafka import KafkaException, Producer

from src.config.kafka_config import kafka_config
from src.utils.monitoring import (
    bytes_produced_total,
    circuit_breaker_state,
    fetch_duration_seconds,
    messages_failed_total,
    messages_produced_total,
    produce_duration_seconds,
)

logger = structlog.get_logger(__name__)


class CircuitState(Enum):
    CLOSED = 0      # Normal — requests pass through
    OPEN = 1        # Failing — requests blocked
    HALF_OPEN = 2   # Testing — one request allowed


class CircuitBreaker:
    """Simple circuit breaker to protect against cascading failures."""

    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: float = 60.0,
        half_open_max_calls: int = 1,
    ):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.half_open_max_calls = half_open_max_calls

        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._last_failure_time: float | None = None
        self._half_open_calls = 0

    @property
    def state(self) -> CircuitState:
        if self._state == CircuitState.OPEN:
            if (
                self._last_failure_time
                and (time.time() - self._last_failure_time) >= self.recovery_timeout
            ):
                self._state = CircuitState.HALF_OPEN
                self._half_open_calls = 0
        return self._state

    def is_open(self) -> bool:
        return self.state == CircuitState.OPEN

    def record_success(self) -> None:
        self._failure_count = 0
        self._state = CircuitState.CLOSED
        self._half_open_calls = 0

    def record_failure(self) -> None:
        self._failure_count += 1
        self._last_failure_time = time.time()
        if self._failure_count >= self.failure_threshold:
            self._state = CircuitState.OPEN


class BaseProducer(ABC):
    """Abstract base class for all FinStreamAI Kafka producers."""

    def __init__(self, producer_name: str, topic: str):
        self.producer_name = producer_name
        self.topic = topic
        self._running = False
        self._producer: Producer | None = None
        self._circuit_breaker = CircuitBreaker()

        self._setup_signal_handlers()

    def _setup_signal_handlers(self) -> None:
        signal.signal(signal.SIGINT, self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)

    def _handle_shutdown(self, signum: int, frame: Any) -> None:
        logger.info("Shutdown signal received", producer=self.producer_name, signal=signum)
        self._running = False

    def _get_producer(self) -> Producer:
        if self._producer is None:
            config = kafka_config.producer_config()
            self._producer = Producer(config)
            logger.info(
                "Kafka producer created",
                producer=self.producer_name,
                bootstrap_servers=kafka_config.KAFKA_BOOTSTRAP_SERVERS,
            )
        return self._producer

    def _delivery_callback(self, err: Any, msg: Any) -> None:
        if err:
            logger.error(
                "Message delivery failed",
                producer=self.producer_name,
                topic=self.topic,
                error=str(err),
            )
            messages_failed_total.labels(
                producer=self.producer_name,
                topic=self.topic,
                error_type=type(err).__name__,
            ).inc()
            self._circuit_breaker.record_failure()
        else:
            messages_produced_total.labels(
                producer=self.producer_name,
                topic=self.topic,
            ).inc()
            bytes_produced_total.labels(
                producer=self.producer_name,
                topic=self.topic,
            ).inc(len(msg.value() or b""))
            self._circuit_breaker.record_success()

        circuit_breaker_state.labels(producer=self.producer_name).set(
            self._circuit_breaker.state.value
        )

    def produce(self, key: str | None, value: bytes, headers: dict | None = None) -> None:
        if self._circuit_breaker.is_open():
            logger.warning(
                "Circuit breaker open — skipping produce",
                producer=self.producer_name,
                topic=self.topic,
            )
            messages_failed_total.labels(
                producer=self.producer_name,
                topic=self.topic,
                error_type="circuit_open",
            ).inc()
            return

        kafka_producer = self._get_producer()
        start = time.perf_counter()
        try:
            kafka_producer.produce(
                topic=self.topic,
                key=key.encode() if key else None,
                value=value,
                headers=headers,
                on_delivery=self._delivery_callback,
            )
            kafka_producer.poll(0)
        except KafkaException as exc:
            logger.error(
                "Failed to produce message",
                producer=self.producer_name,
                topic=self.topic,
                error=str(exc),
            )
            messages_failed_total.labels(
                producer=self.producer_name,
                topic=self.topic,
                error_type=type(exc).__name__,
            ).inc()
            self._circuit_breaker.record_failure()
        finally:
            produce_duration_seconds.labels(
                producer=self.producer_name,
                topic=self.topic,
            ).observe(time.perf_counter() - start)

    def flush(self, timeout: float = 30.0) -> None:
        if self._producer:
            self._producer.flush(timeout=timeout)

    def close(self) -> None:
        logger.info("Closing producer", producer=self.producer_name)
        self.flush()
        self._producer = None

    def run(self) -> None:
        """Main loop — override to customize scheduling."""
        self._running = True
        logger.info("Producer started", producer=self.producer_name, topic=self.topic)
        try:
            while self._running:
                start = time.perf_counter()
                try:
                    data = self.fetch_data()
                    fetch_duration_seconds.labels(
                        producer=self.producer_name,
                        source=self.data_source_name(),
                    ).observe(time.perf_counter() - start)

                    messages = self.transform(data)
                    for key, value in messages:
                        self.produce(key, value)
                except Exception as exc:
                    logger.error(
                        "Producer loop error",
                        producer=self.producer_name,
                        error=str(exc),
                    )
                time.sleep(self.poll_interval_seconds())
        finally:
            self.close()
            logger.info("Producer stopped", producer=self.producer_name)

    # ─── Abstract interface ──────────────────────────────────
    @abstractmethod
    def fetch_data(self) -> Any:
        """Fetch raw data from the external source."""

    @abstractmethod
    def transform(self, raw_data: Any) -> list[tuple[str | None, bytes]]:
        """Transform raw data into (key, value) pairs for Kafka."""

    @abstractmethod
    def data_source_name(self) -> str:
        """Human-readable name of the data source (for metrics)."""

    def poll_interval_seconds(self) -> float:
        """Seconds to wait between fetch cycles. Override to customise."""
        return 1.0
