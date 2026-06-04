"""Tests for query endpoints — role gates."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from httpx import AsyncClient

from src.models.user import User
from tests.conftest import auth_headers


@pytest.mark.asyncio
async def test_market_data_requires_auth(client: AsyncClient):
    resp = await client.get("/api/v1/query/market-data?symbol=AAPL")
    assert resp.status_code == 401


@pytest.mark.asyncio
async def test_market_data_free_user(client: AsyncClient, free_user: User):
    with patch("src.api.v1.query.query_service.query_market_data") as mock_qmd:
        mock_qmd.return_value = {"symbol": "AAPL", "data": [], "cursor": None}
        resp = await client.get(
            "/api/v1/query/market-data?symbol=AAPL",
            headers=auth_headers(free_user),
        )
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_market_data_premium_user(client: AsyncClient, premium_user: User):
    with patch("src.api.v1.query.query_service.query_market_data") as mock_qmd:
        mock_qmd.return_value = {"symbol": "MSFT", "data": [], "cursor": None}
        resp = await client.get(
            "/api/v1/query/market-data?symbol=MSFT",
            headers=auth_headers(premium_user),
        )
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_custom_query_premium_only(client: AsyncClient, free_user: User, premium_user: User):
    payload = {"table": "market_bars_1min", "limit": 10}

    # free_user forbidden
    resp = await client.post("/api/v1/query/custom", json=payload, headers=auth_headers(free_user))
    assert resp.status_code == 403

    # premium allowed (mock DB query)
    with patch("src.api.v1.query.query_service.run_custom_query") as mock_cq:
        mock_cq.return_value = {"rows": [], "cursor": None, "row_count": 0}
        resp = await client.post(
            "/api/v1/query/custom", json=payload, headers=auth_headers(premium_user)
        )
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_sentiment_requires_auth(client: AsyncClient):
    resp = await client.get("/api/v1/query/sentiment?symbol=AAPL")
    assert resp.status_code == 401


@pytest.mark.asyncio
async def test_sentiment_free_user(client: AsyncClient, free_user: User):
    with patch("src.api.v1.query.query_service.query_sentiment") as mock_qs:
        mock_qs.return_value = {"data": [], "cursor": None}
        resp = await client.get(
            "/api/v1/query/sentiment?symbol=AAPL",
            headers=auth_headers(free_user),
        )
    assert resp.status_code == 200
