"""Portfolio tools for the AI advisor."""
from __future__ import annotations

import uuid
from typing import Any

import structlog
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.models.portfolio import PortfolioPosition
from src.models.watchlist import WatchlistItem

logger = structlog.get_logger(__name__)


async def get_positions(user_id: str, db: AsyncSession) -> list[dict[str, Any]]:
    """Return all open portfolio positions for a user."""
    uid = uuid.UUID(user_id)
    result = await db.execute(
        select(PortfolioPosition).where(
            PortfolioPosition.user_id == uid,
            PortfolioPosition.is_open == True,  # noqa: E712
        )
    )
    positions = result.scalars().all()
    return [
        {
            "id": str(p.id),
            "symbol": p.symbol,
            "quantity": p.quantity,
            "avg_cost_basis": p.avg_cost_basis,
            "source": p.source,
        }
        for p in positions
    ]


async def get_watchlist(user_id: str, db: AsyncSession) -> list[str]:
    """Return watchlist symbols for a user."""
    uid = uuid.UUID(user_id)
    result = await db.execute(
        select(WatchlistItem).where(WatchlistItem.user_id == uid)
    )
    items = result.scalars().all()
    return [item.symbol for item in items]


async def calculate_pnl(user_id: str, db: AsyncSession) -> dict[str, Any]:
    """Calculate unrealized PnL based on avg cost basis (no live price fetch)."""
    positions = await get_positions(user_id, db)
    total_cost = sum(p["quantity"] * p["avg_cost_basis"] for p in positions)
    return {
        "user_id": user_id,
        "open_positions": len(positions),
        "total_cost_basis": round(total_cost, 2),
        "note": "Live market prices not fetched — unrealized PnL requires current prices",
    }
