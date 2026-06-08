"""Trading order endpoints."""
from __future__ import annotations

import uuid
from typing import Annotated

import structlog
from fastapi import APIRouter, Depends, HTTPException, Request, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.database import get_db
from src.core.dependencies import get_current_user, get_redis
from src.models.preferences import UserPreference
from src.models.trading import TradeOrder
from src.models.user import User
from src.schemas.trading import KillSwitchResponse, OrderRequest, OrderResponse

logger = structlog.get_logger(__name__)
router = APIRouter(prefix="/trading", tags=["trading"])


@router.post("/orders", status_code=status.HTTP_202_ACCEPTED)
async def submit_order(
    req: OrderRequest,
    user: Annotated[User, Depends(get_current_user)],
    db: Annotated[AsyncSession, Depends(get_db)],
    redis=Depends(get_redis),
    request: Request = None,
):
    chat_model = getattr(request.app.state, "chat_model", None) if request else None

    # Load preferences
    pref_result = await db.execute(
        select(UserPreference).where(UserPreference.user_id == user.id)
    )
    preferences = pref_result.scalar_one_or_none()
    if preferences is None:
        preferences = UserPreference(user_id=user.id)
        db.add(preferences)
        await db.flush()

    from src.agents.trade_executor import execute_trade

    result = await execute_trade(
        user_id=user.id,
        user_role=user.role,
        symbol=req.symbol.upper(),
        side=req.side,
        qty=req.qty,
        order_type=req.order_type,
        db=db,
        redis=redis,
        preferences=preferences,
        chat_model=chat_model,
    )

    return {
        "status": result.status,
        "order_id": result.order_id,
        "reason": result.reason,
        "requires_confirmation": result.requires_confirmation,
    }


@router.post("/orders/{order_id}/confirm", status_code=status.HTTP_204_NO_CONTENT)
async def confirm_order(
    order_id: uuid.UUID,
    user: Annotated[User, Depends(get_current_user)],
    db: Annotated[AsyncSession, Depends(get_db)],
    redis=Depends(get_redis),
):
    result = await db.execute(
        select(TradeOrder).where(
            TradeOrder.id == order_id,
            TradeOrder.user_id == user.id,
        )
    )
    order = result.scalar_one_or_none()
    if order is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Order not found")

    if order.status != "pending_confirmation":
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Order status is '{order.status}', not 'pending_confirmation'",
        )

    # Signal confirmation to waiting trade_executor
    confirm_key = f"agent:confirm:{order_id}"
    await redis.lpush(confirm_key, "confirmed")
    await redis.expire(confirm_key, 600)

    order.status = "confirmed"
    await db.flush()


@router.get("/orders", response_model=list[OrderResponse])
async def list_orders(
    user: Annotated[User, Depends(get_current_user)],
    db: Annotated[AsyncSession, Depends(get_db)],
):
    result = await db.execute(
        select(TradeOrder)
        .where(TradeOrder.user_id == user.id)
        .order_by(TradeOrder.created_at.desc())
    )
    return result.scalars().all()


@router.post("/kill-switch", response_model=KillSwitchResponse)
async def activate_kill_switch(
    user: Annotated[User, Depends(get_current_user)],
    redis=Depends(get_redis),
):
    kill_key = f"agent:killswitch:{user.id}"
    await redis.set(kill_key, "1")
    logger.warning("Kill switch activated", user_id=str(user.id))
    return KillSwitchResponse(
        status="active",
        user_id=str(user.id),
        message="Auto-trading kill switch activated. All pending orders will be rejected.",
    )
