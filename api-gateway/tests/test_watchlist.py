"""Tests for watchlist endpoints."""
from __future__ import annotations

import pytest
from httpx import AsyncClient

from tests.conftest import auth_headers


@pytest.mark.asyncio
async def test_add_watchlist_item(client: AsyncClient, free_user):
    resp = await client.post(
        "/api/v1/watchlist",
        json={"symbol": "AAPL"},
        headers=auth_headers(free_user),
    )
    assert resp.status_code == 201
    data = resp.json()
    assert data["symbol"] == "AAPL"
    assert data["user_id"] == str(free_user.id)


@pytest.mark.asyncio
async def test_add_watchlist_item_uppercase(client: AsyncClient, free_user):
    resp = await client.post(
        "/api/v1/watchlist",
        json={"symbol": "msft"},
        headers=auth_headers(free_user),
    )
    assert resp.status_code == 201
    assert resp.json()["symbol"] == "MSFT"


@pytest.mark.asyncio
async def test_list_watchlist_empty(client: AsyncClient, free_user):
    resp = await client.get("/api/v1/watchlist", headers=auth_headers(free_user))
    assert resp.status_code == 200
    assert resp.json() == []


@pytest.mark.asyncio
async def test_list_watchlist(client: AsyncClient, free_user):
    await client.post("/api/v1/watchlist", json={"symbol": "AAPL"}, headers=auth_headers(free_user))
    await client.post("/api/v1/watchlist", json={"symbol": "TSLA"}, headers=auth_headers(free_user))
    resp = await client.get("/api/v1/watchlist", headers=auth_headers(free_user))
    assert resp.status_code == 200
    symbols = [item["symbol"] for item in resp.json()]
    assert "AAPL" in symbols
    assert "TSLA" in symbols


@pytest.mark.asyncio
async def test_delete_watchlist_item(client: AsyncClient, free_user):
    await client.post("/api/v1/watchlist", json={"symbol": "NVDA"}, headers=auth_headers(free_user))
    resp = await client.delete("/api/v1/watchlist/NVDA", headers=auth_headers(free_user))
    assert resp.status_code == 204

    list_resp = await client.get("/api/v1/watchlist", headers=auth_headers(free_user))
    assert all(item["symbol"] != "NVDA" for item in list_resp.json())


@pytest.mark.asyncio
async def test_delete_nonexistent_symbol_returns_404(client: AsyncClient, free_user):
    resp = await client.delete("/api/v1/watchlist/FAKE", headers=auth_headers(free_user))
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_duplicate_symbol_returns_409(client: AsyncClient, free_user):
    await client.post("/api/v1/watchlist", json={"symbol": "AAPL"}, headers=auth_headers(free_user))
    resp = await client.post(
        "/api/v1/watchlist", json={"symbol": "AAPL"}, headers=auth_headers(free_user)
    )
    assert resp.status_code == 409


@pytest.mark.asyncio
async def test_free_tier_cap(client: AsyncClient, free_user):
    symbols = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA"]
    for sym in symbols:
        r = await client.post(
            "/api/v1/watchlist", json={"symbol": sym}, headers=auth_headers(free_user)
        )
        assert r.status_code == 201

    # 6th symbol → 403
    resp = await client.post(
        "/api/v1/watchlist", json={"symbol": "META"}, headers=auth_headers(free_user)
    )
    assert resp.status_code == 403


@pytest.mark.asyncio
async def test_premium_user_exceeds_free_cap(client: AsyncClient, premium_user):
    symbols = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "META", "NVDA"]
    for sym in symbols:
        r = await client.post(
            "/api/v1/watchlist", json={"symbol": sym}, headers=auth_headers(premium_user)
        )
        assert r.status_code == 201


@pytest.mark.asyncio
async def test_delete_wrong_user_returns_404(client: AsyncClient, free_user, premium_user):
    # free_user adds a symbol
    await client.post("/api/v1/watchlist", json={"symbol": "AAPL"}, headers=auth_headers(free_user))
    # premium_user tries to delete it
    resp = await client.delete("/api/v1/watchlist/AAPL", headers=auth_headers(premium_user))
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_watchlist_unauthenticated(client: AsyncClient):
    resp = await client.get("/api/v1/watchlist")
    assert resp.status_code == 401
