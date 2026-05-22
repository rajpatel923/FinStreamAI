from typing import Any

import structlog

from src.jobs.base_job import BaseJob
from src.processors.anomaly_detector import AnomalyDetector
from src.state.rolling_state import RollingState
from src.utils.monitoring import anomalies_detected_total

logger = structlog.get_logger(__name__)


class AnomalyDetectionJob(BaseJob):
    """Consumes market.ticks.clean, detects anomalies, produces to alerts.anomalies."""

    INPUT_TOPIC = "market.ticks.clean"
    OUTPUT_TOPIC = "alerts.anomalies"

    def __init__(self) -> None:
        super().__init__(
            job_name="anomaly",
            input_topics=[self.INPUT_TOPIC],
            output_topics=[self.OUTPUT_TOPIC],
            input_schema="market_tick",
        )
        self._rolling_state = RollingState()
        self._detector = AnomalyDetector(self._rolling_state)

    def process(self, record: dict, raw_msg: Any) -> list[tuple[str, bytes, bytes]]:
        # Update rolling state first so Z-score is computed with history
        self._rolling_state.update_tick(
            record["symbol"], record["price"], record["volume"], record["timestamp_ms"]
        )

        alerts = self._detector.detect(record)
        outputs = []
        for alert in alerts:
            anomalies_detected_total.labels(
                alert_type=alert["alert_type"],
                severity=alert["severity"],
            ).inc()
            key_bytes = alert["symbol"].encode()
            value_bytes = self._serializer.serialize("anomaly_alert", alert)
            outputs.append((self.OUTPUT_TOPIC, key_bytes, value_bytes))
            logger.info(
                "Anomaly detected",
                symbol=alert["symbol"],
                alert_type=alert["alert_type"],
                severity=alert["severity"],
                message=alert["message"],
            )
        return outputs
