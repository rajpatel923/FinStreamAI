"""Alpaca broker service wrapper."""
from __future__ import annotations

from typing import Any

import structlog

from src.core.config import settings

logger = structlog.get_logger(__name__)


class BrokerService:
    """Wraps alpaca-py TradingClient for order management."""

    def __init__(
        self,
        api_key: str | None = None,
        secret_key: str | None = None,
        paper: bool = True,
    ) -> None:
        self._api_key = api_key or settings.ALPACA_API_KEY
        self._secret_key = secret_key or settings.ALPACA_SECRET_KEY
        self._paper = paper
        self._client = None

    def _get_client(self):
        if self._client is None:
            from alpaca.trading.client import TradingClient

            self._client = TradingClient(
                api_key=self._api_key,
                secret_key=self._secret_key,
                paper=self._paper,
            )
        return self._client

    def place_order(
        self,
        symbol: str,
        side: str,
        qty: float,
        order_type: str = "market",
        paper: bool = True,
    ) -> dict[str, Any]:
        from alpaca.trading.requests import MarketOrderRequest, LimitOrderRequest
        from alpaca.trading.enums import OrderSide, TimeInForce

        client = self._get_client()
        order_side = OrderSide.BUY if side.lower() == "buy" else OrderSide.SELL

        if order_type == "market":
            req = MarketOrderRequest(
                symbol=symbol,
                qty=qty,
                side=order_side,
                time_in_force=TimeInForce.DAY,
            )
        else:
            req = MarketOrderRequest(
                symbol=symbol,
                qty=qty,
                side=order_side,
                time_in_force=TimeInForce.DAY,
            )

        order = client.submit_order(req)
        return {
            "id": str(order.id),
            "symbol": order.symbol,
            "qty": float(order.qty or 0),
            "side": str(order.side),
            "status": str(order.status),
        }

    def get_order(self, order_id: str) -> dict[str, Any]:
        client = self._get_client()
        order = client.get_order_by_id(order_id)
        return {
            "id": str(order.id),
            "symbol": order.symbol,
            "qty": float(order.qty or 0),
            "side": str(order.side),
            "status": str(order.status),
        }

    def get_account(self) -> dict[str, Any]:
        client = self._get_client()
        account = client.get_account()
        return {
            "id": str(account.id),
            "equity": str(account.equity),
            "buying_power": str(account.buying_power),
            "cash": str(account.cash),
        }

    def get_positions(self) -> list[dict[str, Any]]:
        client = self._get_client()
        positions = client.get_all_positions()
        return [
            {
                "symbol": p.symbol,
                "qty": str(p.qty),
                "market_value": str(p.market_value),
                "avg_entry_price": str(p.avg_entry_price),
            }
            for p in positions
        ]
