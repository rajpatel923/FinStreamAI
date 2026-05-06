from pydantic_settings import BaseSettings, SettingsConfigDict


class KafkaConfig(BaseSettings):
    KAFKA_BOOTSTRAP_SERVERS: str = "localhost:9092"
    SCHEMA_REGISTRY_URL: str = "http://localhost:8081"

    # Producer settings
    KAFKA_ACKS: str = "all"
    KAFKA_RETRIES: int = 3
    KAFKA_RETRY_BACKOFF_MS: int = 500
    KAFKA_BATCH_SIZE: int = 16384
    KAFKA_LINGER_MS: int = 5
    KAFKA_COMPRESSION_TYPE: str = "snappy"
    KAFKA_MAX_IN_FLIGHT: int = 5
    KAFKA_ENABLE_IDEMPOTENCE: bool = True

    # Consumer settings
    KAFKA_GROUP_ID: str = "finstreami-consumers"
    KAFKA_AUTO_OFFSET_RESET: str = "earliest"
    KAFKA_ENABLE_AUTO_COMMIT: bool = False
    KAFKA_MAX_POLL_RECORDS: int = 500
    KAFKA_SESSION_TIMEOUT_MS: int = 30000
    KAFKA_HEARTBEAT_INTERVAL_MS: int = 10000

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    def producer_config(self) -> dict:
        return {
            "bootstrap.servers": self.KAFKA_BOOTSTRAP_SERVERS,
            "acks": self.KAFKA_ACKS,
            "retries": self.KAFKA_RETRIES,
            "retry.backoff.ms": self.KAFKA_RETRY_BACKOFF_MS,
            "batch.size": self.KAFKA_BATCH_SIZE,
            "linger.ms": self.KAFKA_LINGER_MS,
            "compression.type": self.KAFKA_COMPRESSION_TYPE,
            "max.in.flight.requests.per.connection": self.KAFKA_MAX_IN_FLIGHT,
            "enable.idempotence": self.KAFKA_ENABLE_IDEMPOTENCE,
        }

    def consumer_config(self, group_id: str | None = None) -> dict:
        return {
            "bootstrap.servers": self.KAFKA_BOOTSTRAP_SERVERS,
            "group.id": group_id or self.KAFKA_GROUP_ID,
            "auto.offset.reset": self.KAFKA_AUTO_OFFSET_RESET,
            "enable.auto.commit": self.KAFKA_ENABLE_AUTO_COMMIT,
            "max.poll.interval.ms": self.KAFKA_MAX_POLL_RECORDS,
            "session.timeout.ms": self.KAFKA_SESSION_TIMEOUT_MS,
            "heartbeat.interval.ms": self.KAFKA_HEARTBEAT_INTERVAL_MS,
        }


kafka_config = KafkaConfig()
