from pydantic_settings import BaseSettings, SettingsConfigDict


class DataSourceConfig(BaseSettings):
    # Polygon.io — real-time and historical market data
    POLYGON_API_KEY: str = ""
    POLYGON_BASE_URL: str = "https://api.polygon.io"
    POLYGON_WS_URL: str = "wss://socket.polygon.io"

    # Alpha Vantage — market data + fundamentals
    ALPHA_VANTAGE_API_KEY: str = ""
    ALPHA_VANTAGE_BASE_URL: str = "https://www.alphavantage.co/query"

    # Finnhub — real-time and historical market data
    FINNHUB_API_KEY: str = ""
    FINNHUB_BASE_URL: str = "https://finnhub.io/api/v1"
    FINNHUB_WS_URL: str = "wss://ws.finnhub.io"

    # News APIs
    NEWS_API_KEY: str = ""
    THE_NEWS_API_KEY: str = ""
    GNEWS_API_KEY: str = ""

    # Reddit
    REDDIT_CLIENT_ID: str = ""
    REDDIT_CLIENT_SECRET: str = ""
    REDDIT_USER_AGENT: str = "FinStreamAI/0.1"

    # Rate limits (requests per minute)
    POLYGON_RATE_LIMIT: int = 100
    ALPHA_VANTAGE_RATE_LIMIT: int = 5
    FINNHUB_RATE_LIMIT: int = 60
    NEWS_API_RATE_LIMIT: int = 100

    # HTTP client settings
    HTTP_TIMEOUT_SECONDS: float = 30.0
    HTTP_MAX_RETRIES: int = 3
    HTTP_RETRY_BACKOFF: float = 1.0

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


data_source_config = DataSourceConfig()
