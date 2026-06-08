"""Portfolio position endpoints."""
from __future__ import annotations

import uuid
from typing import Annotated

import structlog
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.database import get_db
from src.core.dependencies import get_current_user, require_role
from src.models.portfolio import PortfolioPosition
from src.models.user import User
from src.schemas.portfolio import PortfolioSummary, PositionCreate, PositionResponse

logger = structlog.get_logger(__name__)
router = APIRouter(prefix="/portfolio", tags=["portfolio"])


@router.get("/positions", response_model=list[PositionResponse])
async def list_positions(
    user: Annotated[User, Depends(get_current_user)],
    db: Annotated[AsyncSession, Depends(get_db)],
):
    result = await db.execute(
        select(PortfolioPosition)
        .where(PortfolioPosition.user_id == user.id, PortfolioPosition.is_open == True)  # noqa
        .order_by(PortfolioPosition.opened_at.desc())
    )
    return result.scalars().all()


@router.post("/positions", response_model=PositionResponse, status_code=status.HTTP_201_CREATED)
async def add_position(
    req: PositionCreate,
    user: Annotated[User, Depends(require_role("premium_user", "admin"))],
    db: Annotated[AsyncSession, Depends(get_db)],
):
    pos = PortfolioPosition(
        user_id=user.id,
        symbol=req.symbol.upper(),
        quantity=req.quantity,
        avg_cost_basis=req.avg_cost_basis,
        source=req.source,
    )
    db.add(pos)
    await db.flush()
    await db.refresh(pos)
    return pos


@router.delete("/positions/{position_id}", status_code=status.HTTP_204_NO_CONTENT)
async def close_position(
    position_id: uuid.UUID,
    user: Annotated[User, Depends(get_current_user)],
    db: Annotated[AsyncSession, Depends(get_db)],
):
    result = await db.execute(
        select(PortfolioPosition).where(
            PortfolioPosition.id == position_id,
            PortfolioPosition.user_id == user.id,
        )
    )
    pos = result.scalar_one_or_none()
    if pos is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Position not found")

    from datetime import datetime, timezone
    pos.is_open = False
    pos.closed_at = datetime.now(timezone.utc)
    await db.flush()


@router.get("/summary", response_model=PortfolioSummary)
async def portfolio_summary(
    user: Annotated[User, Depends(get_current_user)],
    db: Annotated[AsyncSession, Depends(get_db)],
):
    result = await db.execute(
        select(PortfolioPosition).where(
            PortfolioPosition.user_id == user.id,
            PortfolioPosition.is_open == True,  # noqa
        )
    )
    positions = result.scalars().all()
    total_cost = sum(p.quantity * p.avg_cost_basis for p in positions)

    return PortfolioSummary(
        total_cost_basis=round(total_cost, 2),
        total_market_value=round(total_cost, 2),  # no live price feed
        unrealized_pnl=0.0,
        positions=positions,
    )
