"""Tests for watchlist monitor and monitoring loop."""
from __future__ import annotations

import uuid
from datetime import date
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from tests.conftest import auth_headers


@pytest.mark.asyncio
async def test_no_items_no_notifications(db_session: AsyncSession, mock_redis):
    from src.agents.watchlist_monitor import run_watchlist_scan
    from src.core.config import settings

    count = await run_watchlist_scan(db_session, mock_redis, settings)
    assert count == 0


@pytest.mark.asyncio
async def test_item_no_signal_no_notification(db_session: AsyncSession, mock_redis, premium_user):
    from src.agents.watchlist_monitor import run_watchlist_scan
    from src.core.config import settings
    from src.models.watchlist import WatchlistItem

    item = WatchlistItem(user_id=premium_user.id, symbol="AAPL", alert_on_signal=True)
    db_session.add(item)
    await db_session.flush()
    await db_session.commit()

    # signal_strength = None → 0.0 → below threshold
    mock_redis.hget = AsyncMock(return_value=None)
    mock_redis.get = AsyncMock(return_value=None)

    count = await run_watchlist_scan(db_session, mock_redis, settings)
    assert count == 0


@pytest.mark.asyncio
async def test_high_signal_sends_notification(db_session: AsyncSession, mock_redis, premium_user):
    from src.agents.watchlist_monitor import run_watchlist_scan
    from src.core.config import settings
    from src.models.watchlist import WatchlistItem

    item = WatchlistItem(user_id=premium_user.id, symbol="NVDA", alert_on_signal=True)
    db_session.add(item)
    await db_session.flush()
    await db_session.commit()

    mock_redis.get = AsyncMock(return_value=None)  # no dedup key
    mock_redis.hget = AsyncMock(return_value="0.85")  # high signal strength

    with patch("src.agents.watchlist_monitor._produce_recommendation"):
        with patch("httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=None)
            mock_client.post = AsyncMock(return_value=MagicMock(status_code=204))
            mock_client_cls.return_value = mock_client

            count = await run_watchlist_scan(db_session, mock_redis, settings)

    assert count == 1


@pytest.mark.asyncio
async def test_dedup_prevents_second_notification(
    db_session: AsyncSession, mock_redis, premium_user
):
    from src.agents.watchlist_monitor import run_watchlist_scan
    from src.core.config import settings
    from src.models.watchlist import WatchlistItem

    item = WatchlistItem(user_id=premium_user.id, symbol="TSLA", alert_on_signal=True)
    db_session.add(item)
    await db_session.flush()
    await db_session.commit()

    today = date.today().isoformat()
    dedup_key = f"agent:monitor:dedup:{premium_user.id}:TSLA:{today}"

    # Simulate: dedup key already set
    async def _get(key):
        if key == dedup_key:
            return "1"
        return None

    mock_redis.get = _get

    count = await run_watchlist_scan(db_session, mock_redis, settings)
    assert count == 0


@pytest.mark.asyncio
async def test_digest_service_no_users(db_session: AsyncSession):
    from src.services.digest_service import send_daily_digest
    from src.core.config import settings

    count = await send_daily_digest(db_session, settings)
    assert count == 0


@pytest.mark.asyncio
async def test_digest_service_targets_daily_users(db_session: AsyncSession, premium_user):
    from src.services.digest_service import send_daily_digest
    from src.models.preferences import UserPreference
    from src.core.config import settings

    pref = UserPreference(user_id=premium_user.id, digest_frequency="daily")
    db_session.add(pref)
    await db_session.flush()
    await db_session.commit()

    with patch("src.services.digest_service._send_email", new_callable=AsyncMock):
        count = await send_daily_digest(db_session, settings)

    assert count == 1


@pytest.mark.asyncio
async def test_digest_service_skips_weekly_users(db_session: AsyncSession, premium_user):
    from src.services.digest_service import send_daily_digest
    from src.models.preferences import UserPreference
    from src.core.config import settings

    pref = UserPreference(user_id=premium_user.id, digest_frequency="weekly")
    db_session.add(pref)
    await db_session.flush()
    await db_session.commit()

    count = await send_daily_digest(db_session, settings)
    assert count == 0
