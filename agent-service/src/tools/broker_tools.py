"""Broker tools — thin wrappers over BrokerService."""
from __future__ import annotations

from typing import Any

import structlog

logger = structlog.get_logger(__name__)


def submit_order(
    symbol: str,
    side: str,
    qty: float,
    order_type: str = "market",
    paper: bool = True,
) -> dict[str, Any]:
    """Submit an order via the broker service."""
    from src.services.broker_service import BrokerService

    service = BrokerService()
    try:
        order = service.place_order(
            symbol=symbol,
            side=side,
            qty=qty,
            order_type=order_type,
            paper=paper,
        )
        return {"status": "submitted", "order": order}
    except Exception as exc:
        logger.error("submit_order failed", symbol=symbol, error=str(exc))
        return {"status": "error", "error": str(exc)}


def get_order_status(broker_order_id: str) -> dict[str, Any]:
    """Get status of an existing broker order."""
    from src.services.broker_service import BrokerService

    service = BrokerService()
    try:
        order = service.get_order(broker_order_id)
        return {"status": "ok", "order": order}
    except Exception as exc:
        logger.error("get_order_status failed", order_id=broker_order_id, error=str(exc))
        return {"status": "error", "error": str(exc)}
