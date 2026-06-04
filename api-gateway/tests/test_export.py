"""Tests for export endpoints."""
from __future__ import annotations

import uuid
from unittest.mock import AsyncMock, patch

import pytest
from httpx import AsyncClient

from src.models.user import User
from tests.conftest import auth_headers


@pytest.mark.asyncio
async def test_export_free_user_forbidden(client: AsyncClient, free_user: User):
    resp = await client.post(
        "/api/v1/export",
        json={"query_params": {"table": "market_bars_1min"}, "output_format": "json"},
        headers=auth_headers(free_user),
    )
    assert resp.status_code == 403


@pytest.mark.asyncio
async def test_export_premium_creates_job(client: AsyncClient, premium_user: User):
    with patch("src.api.v1.export.asyncio.create_task"):
        resp = await client.post(
            "/api/v1/export",
            json={"query_params": {"table": "market_bars_1min"}, "output_format": "csv"},
            headers=auth_headers(premium_user),
        )
    assert resp.status_code == 202
    data = resp.json()
    assert data["status"] == "pending"
    assert data["output_format"] == "csv"
    assert "id" in data


@pytest.mark.asyncio
async def test_export_poll_status(client: AsyncClient, premium_user: User):
    with patch("src.api.v1.export.asyncio.create_task"):
        create_resp = await client.post(
            "/api/v1/export",
            json={"query_params": {"table": "market_bars_1min"}, "output_format": "json"},
            headers=auth_headers(premium_user),
        )
    job_id = create_resp.json()["id"]
    resp = await client.get(f"/api/v1/export/{job_id}", headers=auth_headers(premium_user))
    assert resp.status_code == 200
    assert resp.json()["id"] == job_id


@pytest.mark.asyncio
async def test_export_not_found(client: AsyncClient, premium_user: User):
    resp = await client.get(f"/api/v1/export/{uuid.uuid4()}", headers=auth_headers(premium_user))
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_export_one_concurrent_job(client: AsyncClient, premium_user: User):
    with patch("src.api.v1.export.asyncio.create_task"):
        resp1 = await client.post(
            "/api/v1/export",
            json={"query_params": {}, "output_format": "json"},
            headers=auth_headers(premium_user),
        )
        assert resp1.status_code == 202
        # Second job while first is pending
        resp2 = await client.post(
            "/api/v1/export",
            json={"query_params": {}, "output_format": "json"},
            headers=auth_headers(premium_user),
        )
        assert resp2.status_code == 429
