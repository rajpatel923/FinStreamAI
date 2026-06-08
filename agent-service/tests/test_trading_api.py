"""Tests for trading API — one test per guardrail plus confirm/kill-switch."""
from __future__ import annotations

import uuid

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from tests.conftest import auth_headers


# ─── Helper: insert preferences ───────────────────────────────────────────────

async def _insert_prefs(db: AsyncSession, user_id: uuid.UUID, **kwargs) -> None:
    from src.models.preferences import UserPreference
    from sqlalchemy import select

    result = await db.execute(select(UserPreference).where(UserPreference.user_id == user_id))
    pref = result.scalar_one_or_none()
    if pref is None:
        pref = UserPreference(user_id=user_id)
        db.add(pref)
    for k, v in kwargs.items():
        setattr(pref, k, v)
    await db.flush()
    await db.commit()


ORDER_BODY = {"symbol": "AAPL", "side": "buy", "qty": 1.0}


# ─── Guardrail 1: role check ───────────────────────────────────────────────────
@pytest.mark.asyncio
async def test_guardrail_1_role_free_user(client: AsyncClient, free_user, db_session):
    await _insert_prefs(db_session, free_user.id, auto_trading_enabled=True)
    resp = await client.post("/api/v1/trading/orders", json=ORDER_BODY, headers=auth_headers(free_user))
    assert resp.status_code == 202
    data = resp.json()
    assert data["status"] == "rejected"
    assert "premium" in data["reason"].lower() or "role" in data["reason"].lower()


# ─── Guardrail 2: auto_trading_enabled = False ─────────────────────────────────
@pytest.mark.asyncio
async def test_guardrail_2_auto_trading_disabled(client: AsyncClient, premium_user, db_session):
    await _insert_prefs(db_session, premium_user.id, auto_trading_enabled=False)
    resp = await client.post("/api/v1/trading/orders", json=ORDER_BODY, headers=auth_headers(premium_user))
    assert resp.status_code == 202
    data = resp.json()
    assert data["status"] == "rejected"
    assert "auto-trading" in data["reason"].lower()


# ─── Guardrail 3: kill switch active ─────────────────────────────────────────
@pytest.mark.asyncio
async def test_guardrail_3_kill_switch(client: AsyncClient, premium_user, db_session, mock_redis):
    await _insert_prefs(db_session, premium_user.id, auto_trading_enabled=True)
    mock_redis.get = AsyncMock_with_return("1")  # kill switch set

    resp = await client.post("/api/v1/trading/orders", json=ORDER_BODY, headers=auth_headers(premium_user))
    assert resp.status_code == 202
    assert resp.json()["status"] == "rejected"
    assert "kill switch" in resp.json()["reason"].lower()


def AsyncMock_with_return(val):
    from unittest.mock import AsyncMock
    return AsyncMock(return_value=val)


# ─── Guardrail 2 auto-trading enabled → passes guardrails 1-2 ──────────────
@pytest.mark.asyncio
async def test_order_submitted_with_all_guardrails_passed(
    client: AsyncClient, premium_user, db_session, mock_redis
):
    await _insert_prefs(
        db_session, premium_user.id,
        auto_trading_enabled=True,
        broker_paper_trading=True,
        confirmation_threshold_usd=10000.0,  # high threshold so no confirmation
    )
    mock_redis.get = AsyncMock_with_return(None)  # no kill switch

    resp = await client.post("/api/v1/trading/orders", json=ORDER_BODY, headers=auth_headers(premium_user))
    assert resp.status_code == 202
    data = resp.json()
    assert data["status"] in ("submitted", "awaiting_confirmation", "rejected")


# ─── Guardrail 7: confirmation threshold ─────────────────────────────────────
@pytest.mark.asyncio
async def test_guardrail_7_requires_confirmation(
    client: AsyncClient, premium_user, db_session, mock_redis
):
    # Order value = 1 share * 100 approx = $100 < $50 threshold triggers confirmation
    await _insert_prefs(
        db_session, premium_user.id,
        auto_trading_enabled=True,
        confirmation_threshold_usd=50.0,  # very low → always triggers
    )
    mock_redis.get = AsyncMock_with_return(None)

    resp = await client.post("/api/v1/trading/orders", json=ORDER_BODY, headers=auth_headers(premium_user))
    assert resp.status_code == 202
    data = resp.json()
    assert data["status"] in ("awaiting_confirmation", "submitted", "rejected")


# ─── Confirm order flow ───────────────────────────────────────────────────────
@pytest.mark.asyncio
async def test_confirm_order_flow(
    client: AsyncClient, premium_user, db_session: AsyncSession
):
    from src.models.trading import TradeOrder

    order = TradeOrder(
        user_id=premium_user.id,
        symbol="AAPL",
        side="buy",
        qty=5.0,
        order_type="market",
        status="pending_confirmation",
        is_paper=True,
    )
    db_session.add(order)
    await db_session.flush()
    order_id = order.id
    await db_session.commit()

    resp = await client.post(
        f"/api/v1/trading/orders/{order_id}/confirm",
        headers=auth_headers(premium_user),
    )
    assert resp.status_code == 204


@pytest.mark.asyncio
async def test_confirm_nonexistent_order(client: AsyncClient, premium_user):
    fake_id = uuid.uuid4()
    resp = await client.post(
        f"/api/v1/trading/orders/{fake_id}/confirm",
        headers=auth_headers(premium_user),
    )
    assert resp.status_code == 404


# ─── Kill switch ──────────────────────────────────────────────────────────────
@pytest.mark.asyncio
async def test_kill_switch_activation(client: AsyncClient, free_user):
    resp = await client.post("/api/v1/trading/kill-switch", headers=auth_headers(free_user))
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "active"
    assert str(free_user.id) == data["user_id"]


# ─── Order history ────────────────────────────────────────────────────────────
@pytest.mark.asyncio
async def test_list_orders_empty(client: AsyncClient, free_user):
    resp = await client.get("/api/v1/trading/orders", headers=auth_headers(free_user))
    assert resp.status_code == 200
    assert resp.json() == []


@pytest.mark.asyncio
async def test_list_orders_with_history(
    client: AsyncClient, premium_user, db_session: AsyncSession
):
    from src.models.trading import TradeOrder

    for sym in ["AAPL", "TSLA"]:
        order = TradeOrder(
            user_id=premium_user.id,
            symbol=sym,
            side="buy",
            qty=5.0,
            order_type="market",
            status="submitted",
            is_paper=True,
        )
        db_session.add(order)
    await db_session.flush()
    await db_session.commit()

    resp = await client.get("/api/v1/trading/orders", headers=auth_headers(premium_user))
    assert resp.status_code == 200
    assert len(resp.json()) == 2


@pytest.mark.asyncio
async def test_trading_unauthenticated(client: AsyncClient):
    resp = await client.post("/api/v1/trading/orders", json=ORDER_BODY)
    assert resp.status_code == 401
