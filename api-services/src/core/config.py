import json
from typing import Any

from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


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
    POSTGRES_PASSWORD: str = "finstreami123"

    # TimescaleDB
    TIMESCALEDB_HOST: str = "localhost"
    TIMESCALEDB_PORT: int = 5433
    TIMESCALEDB_DB: str = "timescaledb"
    TIMESCALEDB_USER: str = "timescale"
    TIMESCALEDB_PASSWORD: str = "timescale123"

    # Redis
    REDIS_HOST: str = "localhost"
    REDIS_PORT: int = 6379
    REDIS_PASSWORD: str = ""

    # Kafka
    KAFKA_BOOTSTRAP_SERVERS: str = "localhost:9092"
    SCHEMA_REGISTRY_URL: str = "http://localhost:8081"

    # Auth
    JWT_SECRET_KEY: str = "change-me-in-production"
    JWT_ALGORITHM: str = "HS256"

    model_config = SettingsConfigDict(
        env_file=".env",
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
    def redis_url(self) -> str:
        if self.REDIS_PASSWORD:
            return f"redis://:{self.REDIS_PASSWORD}@{self.REDIS_HOST}:{self.REDIS_PORT}/0"
        return f"redis://{self.REDIS_HOST}:{self.REDIS_PORT}/0"


settings = Settings()
