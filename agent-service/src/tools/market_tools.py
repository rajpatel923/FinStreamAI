"""Market data tools for the AI advisor."""
from __future__ import annotations

import json
from typing import Any

import httpx
import structlog

logger = structlog.get_logger(__name__)

_REDIS_CLIENT = None


def _get_redis():
    global _REDIS_CLIENT
    if _REDIS_CLIENT is None:
        import redis

        from src.core.config import settings

        _REDIS_CLIENT = redis.from_url(settings.redis_url, decode_responses=True)
    return _REDIS_CLIENT


def get_market_data(symbol: str) -> dict[str, Any]:
    """Retrieve latest market features for a symbol from Redis feature store."""
    symbol = symbol.upper().strip()
    try:
        r = _get_redis()
        raw = r.hgetall(f"finstreami:features:{symbol}")
        if raw:
            return {"symbol": symbol, "features": raw, "source": "redis"}
        return {"symbol": symbol, "features": {}, "source": "redis", "note": "no data"}
    except Exception as exc:
        logger.warning("get_market_data failed", symbol=symbol, error=str(exc))
        return {"symbol": symbol, "error": str(exc)}


def get_signals(symbol: str) -> dict[str, Any]:
    """Retrieve latest prediction signals for a symbol."""
    symbol = symbol.upper().strip()
    try:
        r = _get_redis()
        raw = r.hgetall(f"finstreami:signals:{symbol}")
        return {"symbol": symbol, "signals": raw}
    except Exception as exc:
        logger.warning("get_signals failed", symbol=symbol, error=str(exc))
        return {"symbol": symbol, "error": str(exc)}


def get_sentiment_score(symbol: str) -> dict[str, Any]:
    """Retrieve sentiment score for a symbol from ai-services."""
    from src.core.config import settings

    symbol = symbol.upper().strip()
    try:
        resp = httpx.get(
            f"{settings.AI_SERVICES_URL}/api/v1/sentiment/symbol/{symbol}",
            timeout=5.0,
        )
        if resp.status_code == 200:
            return resp.json()
        return {"symbol": symbol, "sentiment": None, "note": f"HTTP {resp.status_code}"}
    except Exception as exc:
        logger.warning("get_sentiment_score failed", symbol=symbol, error=str(exc))
        return {"symbol": symbol, "error": str(exc)}
