"""Research tools — news, events, risk metrics via ai-services."""
from __future__ import annotations

from typing import Any

import httpx
import structlog

logger = structlog.get_logger(__name__)


def get_news_events(symbol: str) -> dict[str, Any]:
    """Retrieve recent news events for a symbol from ai-services."""
    from src.core.config import settings

    symbol = symbol.upper().strip()
    try:
        resp = httpx.get(
            f"{settings.AI_SERVICES_URL}/api/v1/events/symbol/{symbol}",
            timeout=5.0,
        )
        if resp.status_code == 200:
            return resp.json()
        return {"symbol": symbol, "events": [], "note": f"HTTP {resp.status_code}"}
    except Exception as exc:
        logger.warning("get_news_events failed", symbol=symbol, error=str(exc))
        return {"symbol": symbol, "error": str(exc)}


def get_risk_metrics(symbols: list[str]) -> dict[str, Any]:
    """Retrieve risk metrics for a list of symbols from ai-services."""
    from src.core.config import settings

    symbols_upper = [s.upper().strip() for s in symbols]
    try:
        resp = httpx.post(
            f"{settings.AI_SERVICES_URL}/api/v1/risk/calculate",
            json={"symbols": symbols_upper},
            timeout=10.0,
        )
        if resp.status_code == 200:
            return resp.json()
        return {"symbols": symbols_upper, "metrics": None, "note": f"HTTP {resp.status_code}"}
    except Exception as exc:
        logger.warning("get_risk_metrics failed", symbols=symbols_upper, error=str(exc))
        return {"symbols": symbols_upper, "error": str(exc)}
