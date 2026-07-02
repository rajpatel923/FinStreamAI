"""Agent service config — extends shared finstream_config settings."""
import sys
from pathlib import Path

_here = Path(__file__).resolve()
_REPO_ROOT = next(
    (p for p in _here.parents if (p / "finstream_config").is_dir()),
    _here.parents[2],
)
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from finstream_config.settings import Settings as _BaseSettings, SettingsConfigDict  # noqa: E402


class AgentSettings(_BaseSettings):
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

    # Broker key encryption (Fernet)
    BROKER_KEY_ENCRYPTION_KEY: str = ""

    # Auto-trading limits
    AUTO_TRADE_CONFIRMATION_THRESHOLD_USD: float = 1000.0
    AUTO_TRADE_MAX_DAILY_LOSS_PCT: float = 0.02
    AUTO_TRADE_MAX_POSITION_SIZE_PCT: float = 0.10

    # Internal cross-service comms
    API_GATEWAY_INTERNAL_URL: str = "http://localhost:8005"
    INTERNAL_PUSH_SECRET: str = ""

    # Personalization
    FREE_TIER_MAX_WATCHLIST: int = 5

    # Monitoring schedule
    WATCHLIST_MONITOR_INTERVAL_MINUTES: int = 15
    DAILY_DIGEST_HOUR: int = 7

    model_config = SettingsConfigDict(
        env_file=(_REPO_ROOT / ".env", ".env"),
        env_file_encoding="utf-8",
        case_sensitive=True,
        extra="ignore",
    )


settings = AgentSettings()
