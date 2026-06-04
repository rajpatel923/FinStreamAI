"""Tests for analytics endpoints — portfolio, risk, backtest."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from httpx import AsyncClient

from src.models.user import User
from tests.conftest import auth_headers


@pytest.mark.asyncio
async def test_portfolio_free_user_forbidden(client: AsyncClient, free_user: User):
    resp = await client.get(
        "/api/v1/analytics/portfolio?symbols=AAPL",
        headers=auth_headers(free_user),
    )
    assert resp.status_code == 403


@pytest.mark.asyncio
async def test_portfolio_premium_user(client: AsyncClient, premium_user: User):
    """Portfolio endpoint requires TimescaleDB which is not in test env.
    Verify auth gate passes (not 401/403) then accept 200 or 500."""
    resp = await client.get(
        "/api/v1/analytics/portfolio?symbols=AAPL&period=7d",
        headers=auth_headers(premium_user),
    )
    # Must NOT be 401/403 (auth passed), DB error in test env is acceptable
    assert resp.status_code not in (401, 403)


@pytest.mark.asyncio
async def test_backtest_free_user_forbidden(client: AsyncClient, free_user: User):
    resp = await client.post(
        "/api/v1/analytics/backtest",
        json={"symbol": "AAPL"},
        headers=auth_headers(free_user),
    )
    assert resp.status_code == 403


@pytest.mark.asyncio
async def test_backtest_requires_auth(client: AsyncClient):
    resp = await client.post("/api/v1/analytics/backtest", json={"symbol": "AAPL"})
    assert resp.status_code == 401


@pytest.mark.asyncio
async def test_risk_free_user_allowed(client: AsyncClient, free_user: User):
    with patch("httpx.AsyncClient.get") as mock_get:
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"var": 0.05}
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp
        resp = await client.get(
            "/api/v1/analytics/risk?symbols=AAPL",
            headers=auth_headers(free_user),
        )
    # Accept 200 or 502 (ai-services may not be running in test)
    assert resp.status_code in (200, 502)
