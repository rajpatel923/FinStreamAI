"""Tests for alert CRUD, SSRF validation, free-tier cap."""
from __future__ import annotations

import uuid
from unittest.mock import AsyncMock, patch

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from src.models.user import User
from tests.conftest import auth_headers

_ALERT_PAYLOAD = {
    "name": "Price Alert",
    "alert_type": "price",
    "symbol": "AAPL",
    "condition": {"operator": "gt", "threshold": 200.0},
    "notification_channels": ["email"],
    "notification_config": {},
}


@pytest.mark.asyncio
async def test_create_alert(client: AsyncClient, free_user: User):
    resp = await client.post("/api/v1/alerts", json=_ALERT_PAYLOAD, headers=auth_headers(free_user))
    assert resp.status_code == 201
    data = resp.json()
    assert data["name"] == "Price Alert"
    assert data["alert_type"] == "price"


@pytest.mark.asyncio
async def test_list_alerts(client: AsyncClient, free_user: User):
    await client.post("/api/v1/alerts", json=_ALERT_PAYLOAD, headers=auth_headers(free_user))
    resp = await client.get("/api/v1/alerts", headers=auth_headers(free_user))
    assert resp.status_code == 200
    assert isinstance(resp.json(), list)


@pytest.mark.asyncio
async def test_update_alert(client: AsyncClient, free_user: User):
    create_resp = await client.post("/api/v1/alerts", json=_ALERT_PAYLOAD, headers=auth_headers(free_user))
    alert_id = create_resp.json()["id"]
    resp = await client.put(
        f"/api/v1/alerts/{alert_id}",
        json={"name": "Updated Alert", "is_active": False},
        headers=auth_headers(free_user),
    )
    assert resp.status_code == 200
    assert resp.json()["name"] == "Updated Alert"
    assert resp.json()["is_active"] is False


@pytest.mark.asyncio
async def test_delete_alert(client: AsyncClient, free_user: User):
    create_resp = await client.post("/api/v1/alerts", json=_ALERT_PAYLOAD, headers=auth_headers(free_user))
    alert_id = create_resp.json()["id"]
    resp = await client.delete(f"/api/v1/alerts/{alert_id}", headers=auth_headers(free_user))
    assert resp.status_code == 204


@pytest.mark.asyncio
async def test_alert_ssrf_webhook_blocked(client: AsyncClient, free_user: User):
    payload = {**_ALERT_PAYLOAD, "notification_channels": ["webhook"],
               "notification_config": {"webhook_url": "http://192.168.1.1/steal"}}
    resp = await client.post("/api/v1/alerts", json=payload, headers=auth_headers(free_user))
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_alert_ssrf_loopback_blocked(client: AsyncClient, free_user: User):
    payload = {**_ALERT_PAYLOAD, "notification_channels": ["webhook"],
               "notification_config": {"webhook_url": "http://127.0.0.1:8080/internal"}}
    resp = await client.post("/api/v1/alerts", json=payload, headers=auth_headers(free_user))
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_alert_free_tier_cap(client: AsyncClient, free_user: User):
    """Free users limited to 3 active alerts."""
    for _ in range(3):
        resp = await client.post("/api/v1/alerts", json=_ALERT_PAYLOAD, headers=auth_headers(free_user))
        assert resp.status_code == 201
    # 4th should be rejected
    resp = await client.post("/api/v1/alerts", json=_ALERT_PAYLOAD, headers=auth_headers(free_user))
    assert resp.status_code == 403


@pytest.mark.asyncio
async def test_alert_invalid_type(client: AsyncClient, free_user: User):
    payload = {**_ALERT_PAYLOAD, "alert_type": "invalid_type"}
    resp = await client.post("/api/v1/alerts", json=payload, headers=auth_headers(free_user))
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_alert_unauthenticated(client: AsyncClient):
    resp = await client.post("/api/v1/alerts", json=_ALERT_PAYLOAD)
    assert resp.status_code == 401


@pytest.mark.asyncio
async def test_delete_other_users_alert(client: AsyncClient, free_user: User, premium_user: User):
    create_resp = await client.post("/api/v1/alerts", json=_ALERT_PAYLOAD, headers=auth_headers(free_user))
    alert_id = create_resp.json()["id"]
    resp = await client.delete(f"/api/v1/alerts/{alert_id}", headers=auth_headers(premium_user))
    assert resp.status_code == 404
