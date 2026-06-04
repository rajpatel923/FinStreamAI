"""Tests for user profile and API key management."""
from __future__ import annotations

import uuid
from unittest.mock import AsyncMock, patch

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from src.models.user import User
from tests.conftest import auth_headers


@pytest.mark.asyncio
async def test_get_me_authenticated(client: AsyncClient, free_user: User):
    resp = await client.get("/api/v1/users/me", headers=auth_headers(free_user))
    assert resp.status_code == 200
    assert resp.json()["email"] == free_user.email


@pytest.mark.asyncio
async def test_get_me_unauthenticated(client: AsyncClient):
    resp = await client.get("/api/v1/users/me")
    assert resp.status_code == 401


@pytest.mark.asyncio
async def test_update_profile(client: AsyncClient, free_user: User):
    resp = await client.put(
        "/api/v1/users/me",
        json={"full_name": "Updated Name"},
        headers=auth_headers(free_user),
    )
    assert resp.status_code == 200
    assert resp.json()["full_name"] == "Updated Name"


@pytest.mark.asyncio
async def test_create_api_key(client: AsyncClient, free_user: User):
    resp = await client.post(
        "/api/v1/users/me/api-keys",
        json={"name": "My Test Key"},
        headers=auth_headers(free_user),
    )
    assert resp.status_code == 201
    data = resp.json()
    assert "full_key" in data
    assert data["full_key"].startswith("fsk_")
    assert len(data["key_prefix"]) == 10


@pytest.mark.asyncio
async def test_list_api_keys(client: AsyncClient, free_user: User):
    # Create a key first
    await client.post(
        "/api/v1/users/me/api-keys",
        json={"name": "List Test Key"},
        headers=auth_headers(free_user),
    )
    resp = await client.get("/api/v1/users/me/api-keys", headers=auth_headers(free_user))
    assert resp.status_code == 200
    assert isinstance(resp.json(), list)
    assert len(resp.json()) >= 1


@pytest.mark.asyncio
async def test_revoke_api_key(client: AsyncClient, free_user: User):
    # Create
    create_resp = await client.post(
        "/api/v1/users/me/api-keys",
        json={"name": "Revoke Me"},
        headers=auth_headers(free_user),
    )
    key_id = create_resp.json()["id"]
    # Revoke
    resp = await client.delete(
        f"/api/v1/users/me/api-keys/{key_id}",
        headers=auth_headers(free_user),
    )
    assert resp.status_code == 204


@pytest.mark.asyncio
async def test_revoke_nonexistent_key(client: AsyncClient, free_user: User):
    resp = await client.delete(
        f"/api/v1/users/me/api-keys/{uuid.uuid4()}",
        headers=auth_headers(free_user),
    )
    assert resp.status_code == 404
