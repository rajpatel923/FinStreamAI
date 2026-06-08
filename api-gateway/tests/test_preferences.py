"""Tests for user preferences endpoints."""
from __future__ import annotations

import pytest
from httpx import AsyncClient

from tests.conftest import auth_headers


@pytest.mark.asyncio
async def test_get_preferences_creates_defaults(client: AsyncClient, free_user):
    resp = await client.get("/api/v1/users/me/preferences", headers=auth_headers(free_user))
    assert resp.status_code == 200
    data = resp.json()
    assert data["risk_tolerance"] == "moderate"
    assert data["investment_horizon"] == "medium_term"
    assert data["digest_frequency"] == "daily"
    assert data["auto_trading_enabled"] is False
    assert data["broker_paper_trading"] is True
    assert data["user_id"] == str(free_user.id)


@pytest.mark.asyncio
async def test_get_preferences_idempotent(client: AsyncClient, free_user):
    resp1 = await client.get("/api/v1/users/me/preferences", headers=auth_headers(free_user))
    resp2 = await client.get("/api/v1/users/me/preferences", headers=auth_headers(free_user))
    assert resp1.status_code == 200
    assert resp2.status_code == 200
    assert resp1.json()["id"] == resp2.json()["id"]


@pytest.mark.asyncio
async def test_update_preferences(client: AsyncClient, free_user):
    resp = await client.put(
        "/api/v1/users/me/preferences",
        json={"risk_tolerance": "aggressive", "digest_frequency": "weekly"},
        headers=auth_headers(free_user),
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["risk_tolerance"] == "aggressive"
    assert data["digest_frequency"] == "weekly"


@pytest.mark.asyncio
async def test_update_auto_trading(client: AsyncClient, premium_user):
    resp = await client.put(
        "/api/v1/users/me/preferences",
        json={"auto_trading_enabled": True, "broker_paper_trading": True},
        headers=auth_headers(premium_user),
    )
    assert resp.status_code == 200
    assert resp.json()["auto_trading_enabled"] is True


@pytest.mark.asyncio
async def test_invalid_risk_tolerance_returns_422(client: AsyncClient, free_user):
    resp = await client.put(
        "/api/v1/users/me/preferences",
        json={"risk_tolerance": "yolo"},
        headers=auth_headers(free_user),
    )
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_invalid_investment_horizon_returns_422(client: AsyncClient, free_user):
    resp = await client.put(
        "/api/v1/users/me/preferences",
        json={"investment_horizon": "forever"},
        headers=auth_headers(free_user),
    )
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_preferences_unauthenticated(client: AsyncClient):
    resp = await client.get("/api/v1/users/me/preferences")
    assert resp.status_code == 401
