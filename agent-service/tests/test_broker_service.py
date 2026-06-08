"""Tests for BrokerService (Alpaca mocked)."""
from __future__ import annotations

import uuid
from unittest.mock import MagicMock, patch

import pytest


@pytest.mark.asyncio
async def test_broker_service_init():
    from src.services.broker_service import BrokerService

    svc = BrokerService(api_key="test-key", secret_key="test-secret", paper=True)
    assert svc._api_key == "test-key"
    assert svc._paper is True


@pytest.mark.asyncio
async def test_broker_place_order_paper(mock_alpaca):
    from src.services.broker_service import BrokerService

    with patch("src.services.broker_service.BrokerService._get_client", return_value=mock_alpaca):
        svc = BrokerService()
        svc._client = mock_alpaca

        result = svc.place_order("AAPL", "buy", 10.0, paper=True)

    assert "id" in result
    assert result["symbol"] == "AAPL"


@pytest.mark.asyncio
async def test_broker_get_order(mock_alpaca):
    from src.services.broker_service import BrokerService

    order_id = str(uuid.uuid4())
    order_mock = MagicMock()
    order_mock.id = order_id
    order_mock.symbol = "AAPL"
    order_mock.qty = 10.0
    order_mock.side = "buy"
    order_mock.status = "filled"
    mock_alpaca.get_order_by_id.return_value = order_mock

    svc = BrokerService()
    svc._client = mock_alpaca

    result = svc.get_order(order_id)
    assert result["id"] == order_id
    assert result["status"] == "filled"


@pytest.mark.asyncio
async def test_broker_get_account(mock_alpaca):
    from src.services.broker_service import BrokerService

    svc = BrokerService()
    svc._client = mock_alpaca

    account = svc.get_account()
    assert "equity" in account
    assert "buying_power" in account


@pytest.mark.asyncio
async def test_broker_get_positions_empty(mock_alpaca):
    from src.services.broker_service import BrokerService

    svc = BrokerService()
    svc._client = mock_alpaca
    mock_alpaca.get_all_positions.return_value = []

    positions = svc.get_positions()
    assert positions == []
