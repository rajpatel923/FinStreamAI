import json
from pathlib import Path
from typing import Any

from pydantic import AliasChoices, Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

_REPO_ROOT = Path(__file__).resolve().parents[1]


class Settings(BaseSettings):
    # App
    APP_NAME: str = "FinStreamAI API"
    API_V1_PREFIX: str = "/api/v1"
    DEBUG: bool = False
    LOG_LEVEL: str = "INFO"
    CORS_ORIGINS: list[str] = []

    # PostgreSQL
    POSTGRES_HOST: str = "localhost"
    POSTGRES_PORT: int = 5432
    POSTGRES_DB: str = "finstreami"
    POSTGRES_USER: str = "finstreami"
    POSTGRES_PASSWORD: str = ""

    # TimescaleDB
    TIMESCALEDB_HOST: str = "localhost"
    TIMESCALEDB_PORT: int = 5433
    TIMESCALEDB_DB: str = "timescaledb"
    TIMESCALEDB_USER: str = "timescale"
    TIMESCALEDB_PASSWORD: str = ""

    # Redis
    REDIS_HOST: str = "localhost"
    REDIS_PORT: int = 6379
    REDIS_PASSWORD: str = ""

    # Neo4j
    NEO4J_URI: str = "bolt://localhost:7687"
    NEO4J_USER: str = "neo4j"
    NEO4J_PASSWORD: str = ""

    # Kafka
    KAFKA_BOOTSTRAP_SERVERS: str = Field(
        default="localhost:9092",
        validation_alias=AliasChoices("KAFKA_BOOTSTRAP_SERVERS", "KAFKA_BROKERS"),
    )
    SCHEMA_REGISTRY_URL: str = "http://localhost:8081"

    # Kafka producer settings
    KAFKA_ACKS: str = "all"
    KAFKA_RETRIES: int = 3
    KAFKA_RETRY_BACKOFF_MS: int = 500
    KAFKA_BATCH_SIZE: int = 16384
    KAFKA_LINGER_MS: int = 5
    KAFKA_COMPRESSION_TYPE: str = "snappy"
    KAFKA_MAX_IN_FLIGHT: int = 5
    KAFKA_ENABLE_IDEMPOTENCE: bool = True

    # Kafka consumer settings
    KAFKA_GROUP_ID: str = "finstreami-consumers"
    KAFKA_AUTO_OFFSET_RESET: str = "earliest"
    KAFKA_ENABLE_AUTO_COMMIT: bool = False
    KAFKA_MAX_POLL_RECORDS: int = 500
    KAFKA_SESSION_TIMEOUT_MS: int = 30000
    KAFKA_HEARTBEAT_INTERVAL_MS: int = 10000

    # Market data APIs
    POLYGON_API_KEY: str = Field(
        default="",
        validation_alias=AliasChoices("POLYGON_API_KEY", "MARKET_API_KEY"),
    )
    POLYGON_BASE_URL: str = "https://api.polygon.io"
    POLYGON_WS_URL: str = Field(
        default="wss://socket.polygon.io",
        validation_alias=AliasChoices("POLYGON_WS_URL", "MARKET_WS_URL"),
    )
    ALPHA_VANTAGE_API_KEY: str = ""
    ALPHA_VANTAGE_BASE_URL: str = Field(
        default="https://www.alphavantage.co/query",
        validation_alias=AliasChoices("ALPHA_VANTAGE_BASE_URL", "ALPHAVANTAGE_URL"),
    )
    FINNHUB_API_KEY: str = ""
    FINNHUB_BASE_URL: str = "https://finnhub.io/api/v1"
    FINNHUB_WS_URL: str = "wss://ws.finnhub.io"

    # News APIs
    NEWS_API_KEY: str = ""
    NEWS_API_BASE_URL: str = Field(
        default="https://newsapi.org/v2/everything",
        validation_alias=AliasChoices("NEWS_API_BASE_URL", "NEWS_API_URL"),
    )
    THE_NEWS_API_KEY: str = ""
    GNEWS_API_KEY: str = ""

    # Social APIs
    REDDIT_CLIENT_ID: str = ""
    REDDIT_CLIENT_SECRET: str = Field(
        default="",
        validation_alias=AliasChoices("REDDIT_CLIENT_SECRET", "REDDIT_API_KEY"),
    )
    REDDIT_USER_AGENT: str = "FinStreamAI/0.1"
    TWITTER_API_KEY: str = ""

    # Rate limits
    POLYGON_RATE_LIMIT: int = 100
    ALPHA_VANTAGE_RATE_LIMIT: int = 5
    FINNHUB_RATE_LIMIT: int = 60
    NEWS_API_RATE_LIMIT: int = 100

    # HTTP client settings
    HTTP_TIMEOUT_SECONDS: float = 30.0
    HTTP_MAX_RETRIES: int = 3
    HTTP_RETRY_BACKOFF: float = 1.0

    # Mock / dev mode
    USE_MOCK_DATA: bool = False
    WATCHED_SYMBOLS: str = "AAPL,MSFT,GOOGL,AMZN,TSLA,META,NVDA,JPM"
    NEWS_FINANCIAL_KEYWORDS: str = "stock,earnings,fed,market,trading"
    REDDIT_SUBREDDITS: str = "wallstreetbets,stocks,investing"

    # AWS / object storage
    AWS_REGION: str = "us-east-1"
    AWS_ACCESS_KEY_ID: str = ""
    AWS_SECRET_ACCESS_KEY: str = ""
    S3_BUCKET_NAME: str = ""

    # Auth / security
    JWT_SECRET_KEY: str = ""
    JWT_ALGORITHM: str = "HS256"
    ENCRYPTION_KEY: str = ""

    # Monitoring
    PROMETHEUS_URL: str = "http://localhost:9090"
    GRAFANA_URL: str = "http://localhost:3001"
    JAEGER_ENDPOINT: str = "http://localhost:14268/api/traces"
    METRICS_PORT: int = 8001

    # ─── AI Services ─────────────────────────────────────────────
    ANTHROPIC_API_KEY: str = ""
    AI_SERVICES_PORT: int = 8003
    CHROMA_PERSIST_DIR: str = "./chroma_store"
    XGB_RETRAIN_INTERVAL_S: int = 3600
    XGB_LOOKBACK_DAYS: int = 30
    AI_MIN_TRAIN_SAMPLES: int = 50

    # ─── API Gateway ─────────────────────────────────────────────
    GATEWAY_PORT: int = 8005
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 30
    REFRESH_TOKEN_EXPIRE_DAYS: int = 7
    REFRESH_TOKEN_ABSOLUTE_MAX_DAYS: int = 30
    GOOGLE_CLIENT_ID: str = ""
    GOOGLE_CLIENT_SECRET: str = ""
    GITHUB_CLIENT_ID: str = ""
    GITHUB_CLIENT_SECRET: str = ""
    OAUTH_REDIRECT_BASE_URL: str = "http://localhost:8005"
    SENDGRID_API_KEY: str = ""
    SENDGRID_FROM_EMAIL: str = "noreply@finstreami.io"
    TWILIO_ACCOUNT_SID: str = ""
    TWILIO_AUTH_TOKEN: str = ""
    TWILIO_FROM_NUMBER: str = ""
    RATE_LIMIT_FREE_PER_MINUTE: int = 60
    RATE_LIMIT_PREMIUM_PER_MINUTE: int = 600
    RATE_LIMIT_ADMIN_PER_MINUTE: int = 6000
    RATE_LIMIT_UNAUTH_PER_MINUTE: int = 20
    RATE_LIMIT_LOGIN_PER_MINUTE: int = 5
    RATE_LIMIT_BACKTEST_PER_HOUR: int = 5
    LOGIN_MAX_ATTEMPTS: int = 10
    LOCKOUT_DURATION_MINUTES: int = 15
    FREE_TIER_MAX_ALERTS: int = 3
    FREE_TIER_MARKET_DATA_HOURS: int = 24
    FREE_TIER_MAX_SYMBOLS: int = 10
    PREMIUM_EXPORT_MAX_BYTES: int = 1_073_741_824
    EXPORT_PRESIGNED_URL_EXPIRES_S: int = 3600
    AI_SERVICES_URL: str = "http://localhost:8003"
    DATA_LAKE_URL: str = "http://localhost:8004"
    OTEL_ENABLED: bool = False
    OTEL_EXPORTER_OTLP_ENDPOINT: str = "http://localhost:4318"
    OTEL_SERVICE_NAME: str = "api-gateway"

    # ─── Agent Service (Phase 7) ─────────────────────────────────
    AGENT_SERVICE_PORT: int = 8006
    ANTHROPIC_MODEL: str = "claude-sonnet-4-6"

    # LLM provider: anthropic | openrouter | llama_cpp
    LLM_PROVIDER: str = "anthropic"

    # OpenRouter
    OPENROUTER_API_KEY: str = ""
    OPENROUTER_BASE_URL: str = "https://openrouter.ai/api/v1"
    OPENROUTER_MODEL: str = "meta-llama/llama-3.3-70b-instruct"

    # llama.cpp local server (llama-server --port 8080)
    LLAMA_CPP_BASE_URL: str = "http://localhost:8080/v1"
    LLAMA_CPP_MODEL: str = "local-model"
    LLAMA_CPP_API_KEY: str = "none"

    # Alpaca broker
    ALPACA_API_KEY: str = ""
    ALPACA_SECRET_KEY: str = ""
    ALPACA_BASE_URL: str = "https://paper-api.alpaca.markets"

    # Broker key encryption (Fernet — 32-byte base64 key)
    BROKER_KEY_ENCRYPTION_KEY: str = ""

    # Auto-trading limits
    AUTO_TRADE_CONFIRMATION_THRESHOLD_USD: float = 1000.0
    AUTO_TRADE_MAX_DAILY_LOSS_PCT: float = 0.02
    AUTO_TRADE_MAX_POSITION_SIZE_PCT: float = 0.10

    # Internal cross-service comms
    API_GATEWAY_INTERNAL_URL: str = "http://localhost:8005"
    INTERNAL_PUSH_SECRET: str = ""

    # Personalization limits
    FREE_TIER_MAX_WATCHLIST: int = 5

    # Monitoring schedule
    WATCHLIST_MONITOR_INTERVAL_MINUTES: int = 15
    DAILY_DIGEST_HOUR: int = 7  # 7 AM UTC

    # ─── Data Lake ───────────────────────────────────────────────
    MINIO_ENDPOINT: str = "http://localhost:9000"
    MINIO_ROOT_USER: str = "minioadmin"
    MINIO_ROOT_PASSWORD: str = "minioadmin123"
    DATALAKE_BUCKET_NAME: str = "finstreami-datalake"
    DATA_LAKE_METRICS_PORT: int = 8004

    # ─── Stream Processing ────────────────────────────────────────
    STREAM_PROCESSING_GROUP_PREFIX: str = "stream"
    WINDOW_GRACE_PERIOD_MS: int = 30_000
    DEDUP_WINDOW_MS: int = 60_000
    ANOMALY_ZSCORE_HIGH: float = 3.0
    ANOMALY_ZSCORE_MEDIUM: float = 2.0
    VOLUME_SPIKE_HIGH_MULT: float = 3.0
    VOLUME_SPIKE_MEDIUM_MULT: float = 2.0
    MOMENTUM_THRESHOLD_PCT: float = 5.0
    JOIN_WINDOW_MS: int = 300_000
    FEATURE_STORE_TTL_S: int = 300
    FEATURE_MIN_BARS: int = 20
    INDICATOR_DEQUE_MAXLEN: int = 200
    RSI_OVERBOUGHT: float = 70.0
    RSI_OVERSOLD: float = 30.0
    SIGNAL_DEDUP_WINDOW_S: int = 300
    STREAM_METRICS_PORT: int = 8002

    model_config = SettingsConfigDict(
        env_file=(_REPO_ROOT / ".env", ".env"),
        env_file_encoding="utf-8",
        case_sensitive=True,
        extra="ignore",
    )

    @field_validator("CORS_ORIGINS", mode="before")
    @classmethod
    def parse_cors_origins(cls, v: Any) -> list[str]:
        if isinstance(v, list):
            return v
        if isinstance(v, str):
            try:
                return json.loads(v)
            except json.JSONDecodeError:
                return [origin.strip() for origin in v.split(",") if origin.strip()]
        return []

    @staticmethod
    def _csv(value: str) -> list[str]:
        return [item.strip() for item in value.split(",") if item.strip()]

    @property
    def postgres_url(self) -> str:
        return (
            f"postgresql+asyncpg://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}"
            f"@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"
        )

    @property
    def timescaledb_url(self) -> str:
        return (
            f"postgresql+asyncpg://{self.TIMESCALEDB_USER}:{self.TIMESCALEDB_PASSWORD}"
            f"@{self.TIMESCALEDB_HOST}:{self.TIMESCALEDB_PORT}/{self.TIMESCALEDB_DB}"
        )

    @property
    def timescaledb_sync_url(self) -> str:
        return (
            f"postgresql://{self.TIMESCALEDB_USER}:{self.TIMESCALEDB_PASSWORD}"
            f"@{self.TIMESCALEDB_HOST}:{self.TIMESCALEDB_PORT}/{self.TIMESCALEDB_DB}"
        )

    @property
    def redis_url(self) -> str:
        if self.REDIS_PASSWORD:
            return f"redis://:{self.REDIS_PASSWORD}@{self.REDIS_HOST}:{self.REDIS_PORT}/0"
        return f"redis://{self.REDIS_HOST}:{self.REDIS_PORT}/0"

    @property
    def watched_symbols_list(self) -> list[str]:
        return self._csv(self.WATCHED_SYMBOLS)

    @property
    def reddit_subreddits_list(self) -> list[str]:
        return self._csv(self.REDDIT_SUBREDDITS)

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
            "session.timeout.ms": self.KAFKA_SESSION_TIMEOUT_MS,
            "heartbeat.interval.ms": self.KAFKA_HEARTBEAT_INTERVAL_MS,
        }


settings = Settings()
kafka_config = settings
data_source_config = settings
