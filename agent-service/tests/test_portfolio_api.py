"""Tests for portfolio API endpoints."""
from __future__ import annotations

import uuid

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from tests.conftest import auth_headers


@pytest.mark.asyncio
async def test_list_positions_empty(client: AsyncClient, free_user):
    resp = await client.get("/api/v1/portfolio/positions", headers=auth_headers(free_user))
    assert resp.status_code == 200
    assert resp.json() == []


@pytest.mark.asyncio
async def test_add_position_premium(client: AsyncClient, premium_user):
    resp = await client.post(
        "/api/v1/portfolio/positions",
        json={"symbol": "AAPL", "quantity": 10, "avg_cost_basis": 150.0},
        headers=auth_headers(premium_user),
    )
    assert resp.status_code == 201
    data = resp.json()
    assert data["symbol"] == "AAPL"
    assert data["quantity"] == 10.0
    assert data["is_open"] is True


@pytest.mark.asyncio
async def test_add_position_free_user_returns_403(client: AsyncClient, free_user):
    resp = await client.post(
        "/api/v1/portfolio/positions",
        json={"symbol": "TSLA", "quantity": 5, "avg_cost_basis": 200.0},
        headers=auth_headers(free_user),
    )
    assert resp.status_code == 403


@pytest.mark.asyncio
async def test_close_position(client: AsyncClient, premium_user, db_session: AsyncSession):
    from src.models.portfolio import PortfolioPosition

    pos = PortfolioPosition(
        user_id=premium_user.id,
        symbol="MSFT",
        quantity=5.0,
        avg_cost_basis=300.0,
    )
    db_session.add(pos)
    await db_session.flush()
    pos_id = pos.id
    await db_session.commit()

    resp = await client.delete(
        f"/api/v1/portfolio/positions/{pos_id}",
        headers=auth_headers(premium_user),
    )
    assert resp.status_code == 204


@pytest.mark.asyncio
async def test_close_nonexistent_position(client: AsyncClient, premium_user):
    fake_id = uuid.uuid4()
    resp = await client.delete(
        f"/api/v1/portfolio/positions/{fake_id}",
        headers=auth_headers(premium_user),
    )
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_portfolio_summary_empty(client: AsyncClient, free_user):
    resp = await client.get("/api/v1/portfolio/summary", headers=auth_headers(free_user))
    assert resp.status_code == 200
    data = resp.json()
    assert data["total_cost_basis"] == 0.0
    assert data["positions"] == []


@pytest.mark.asyncio
async def test_portfolio_summary_with_positions(
    client: AsyncClient, premium_user, db_session: AsyncSession
):
    from src.models.portfolio import PortfolioPosition

    positions = [
        PortfolioPosition(user_id=premium_user.id, symbol="AAPL", quantity=10, avg_cost_basis=150.0),
        PortfolioPosition(user_id=premium_user.id, symbol="MSFT", quantity=5, avg_cost_basis=300.0),
    ]
    for p in positions:
        db_session.add(p)
    await db_session.flush()
    await db_session.commit()

    resp = await client.get("/api/v1/portfolio/summary", headers=auth_headers(premium_user))
    assert resp.status_code == 200
    data = resp.json()
    # 10*150 + 5*300 = 1500 + 1500 = 3000
    assert data["total_cost_basis"] == 3000.0
    assert len(data["positions"]) == 2


@pytest.mark.asyncio
async def test_portfolio_unauthenticated(client: AsyncClient):
    resp = await client.get("/api/v1/portfolio/positions")
    assert resp.status_code == 401
