"""Gateway config — extends shared finstream_config settings."""
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[4]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from finstream_config.settings import Settings as _BaseSettings, SettingsConfigDict  # noqa: E402


class GatewaySettings(_BaseSettings):
    # ─── Gateway ──────────────────────────────────────────────────
    GATEWAY_PORT: int = 8005
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 30
    REFRESH_TOKEN_EXPIRE_DAYS: int = 7
    REFRESH_TOKEN_ABSOLUTE_MAX_DAYS: int = 30

    # ─── OAuth ────────────────────────────────────────────────────
    GOOGLE_CLIENT_ID: str = ""
    GOOGLE_CLIENT_SECRET: str = ""
    GITHUB_CLIENT_ID: str = ""
    GITHUB_CLIENT_SECRET: str = ""
    OAUTH_REDIRECT_BASE_URL: str = "http://localhost:8005"

    # ─── Notifications ────────────────────────────────────────────
    SENDGRID_API_KEY: str = ""
    SENDGRID_FROM_EMAIL: str = "noreply@finstreami.io"
    TWILIO_ACCOUNT_SID: str = ""
    TWILIO_AUTH_TOKEN: str = ""
    TWILIO_FROM_NUMBER: str = ""

    # ─── Rate limits ──────────────────────────────────────────────
    RATE_LIMIT_FREE_PER_MINUTE: int = 60
    RATE_LIMIT_PREMIUM_PER_MINUTE: int = 600
    RATE_LIMIT_ADMIN_PER_MINUTE: int = 6000
    RATE_LIMIT_UNAUTH_PER_MINUTE: int = 20
    RATE_LIMIT_LOGIN_PER_MINUTE: int = 5
    RATE_LIMIT_BACKTEST_PER_HOUR: int = 5

    # ─── Account lockout ──────────────────────────────────────────
    LOGIN_MAX_ATTEMPTS: int = 10
    LOCKOUT_DURATION_MINUTES: int = 15

    # ─── Free tier limits ─────────────────────────────────────────
    FREE_TIER_MAX_ALERTS: int = 3
    FREE_TIER_MARKET_DATA_HOURS: int = 24
    FREE_TIER_MAX_SYMBOLS: int = 10

    # ─── Export limits ────────────────────────────────────────────
    PREMIUM_EXPORT_MAX_BYTES: int = 1_073_741_824  # 1 GB
    EXPORT_PRESIGNED_URL_EXPIRES_S: int = 3600

    # ─── Downstream services ──────────────────────────────────────
    AI_SERVICES_URL: str = "http://localhost:8003"
    DATA_LAKE_URL: str = "http://localhost:8004"

    # ─── OpenTelemetry ────────────────────────────────────────────
    OTEL_ENABLED: bool = False
    OTEL_EXPORTER_OTLP_ENDPOINT: str = "http://localhost:4318"
    OTEL_SERVICE_NAME: str = "api-gateway"

    model_config = SettingsConfigDict(
        env_file=(_REPO_ROOT / ".env", ".env"),
        env_file_encoding="utf-8",
        case_sensitive=True,
        extra="ignore",
    )


settings = GatewaySettings()
