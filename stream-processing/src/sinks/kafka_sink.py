import structlog
from confluent_kafka import KafkaException, Producer

from src.config import settings
from src.utils.monitoring import bytes_produced_total, messages_failed_total, messages_produced_total

logger = structlog.get_logger(__name__)


class KafkaSink:
    """Thin confluent-kafka Producer wrapper with delivery metrics."""

    def __init__(self, job_name: str) -> None:
        self._job_name = job_name
        self._producer: Producer | None = None

    def _get_producer(self) -> Producer:
        if self._producer is None:
            self._producer = Producer(settings.producer_config())
            logger.info("KafkaSink producer created", job=self._job_name)
        return self._producer

    def produce(self, topic: str, key: bytes | None, value: bytes) -> None:
        producer = self._get_producer()
        try:
            producer.produce(
                topic=topic,
                key=key,
                value=value,
                on_delivery=self._on_delivery,
            )
            producer.poll(0)
        except KafkaException as exc:
            logger.error("KafkaSink produce failed", job=self._job_name, topic=topic, error=str(exc))
            messages_failed_total.labels(job=self._job_name, error_type=type(exc).__name__).inc()

    def _on_delivery(self, err, msg) -> None:
        if err:
            logger.error("Delivery failed", job=self._job_name, error=str(err))
            messages_failed_total.labels(job=self._job_name, error_type="delivery_error").inc()
        else:
            messages_produced_total.labels(job=self._job_name, topic=msg.topic()).inc()
            bytes_produced_total.labels(job=self._job_name, topic=msg.topic()).inc(
                len(msg.value() or b"")
            )

    def flush(self, timeout: float = 30.0) -> None:
        if self._producer:
            self._producer.flush(timeout=timeout)

    def close(self) -> None:
        self.flush()
        self._producer = None
