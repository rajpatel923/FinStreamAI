"""Alert CRUD and background evaluation loop."""
from __future__ import annotations

import asyncio
import uuid
from datetime import datetime, timezone
from typing import Any

import structlog
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.config import settings
from src.models.alert import Alert
from src.schemas.alert import AlertCreate, AlertUpdate

logger = structlog.get_logger(__name__)


async def create_alert(
    user_id: uuid.UUID,
    req: AlertCreate,
    db: AsyncSession,
    user_role: str,
) -> Alert:
    if user_role == "free_user":
        count_result = await db.execute(
            select(Alert).where(Alert.user_id == user_id, Alert.is_active == True)  # noqa: E712
        )
        if len(count_result.scalars().all()) >= settings.FREE_TIER_MAX_ALERTS:
            from fastapi import HTTPException, status
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Free tier limited to {settings.FREE_TIER_MAX_ALERTS} active alerts",
            )

    alert = Alert(
        user_id=user_id,
        name=req.name,
        alert_type=req.alert_type,
        symbol=req.symbol,
        condition=req.condition,
        notification_channels=req.notification_channels,
        notification_config=req.notification_config,
    )
    db.add(alert)
    await db.flush()
    await db.refresh(alert)
    return alert


async def list_alerts(user_id: uuid.UUID, db: AsyncSession) -> list[Alert]:
    result = await db.execute(
        select(Alert).where(Alert.user_id == user_id).order_by(Alert.created_at.desc())
    )
    return result.scalars().all()


async def get_alert(alert_id: uuid.UUID, user_id: uuid.UUID, db: AsyncSession) -> Alert | None:
    result = await db.execute(
        select(Alert).where(Alert.id == alert_id, Alert.user_id == user_id)
    )
    return result.scalar_one_or_none()


async def update_alert(
    alert_id: uuid.UUID,
    user_id: uuid.UUID,
    req: AlertUpdate,
    db: AsyncSession,
) -> Alert:
    from fastapi import HTTPException, status

    alert = await get_alert(alert_id, user_id, db)
    if alert is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Alert not found")

    if req.name is not None:
        alert.name = req.name
    if req.is_active is not None:
        alert.is_active = req.is_active
    if req.condition is not None:
        alert.condition = req.condition
    if req.notification_channels is not None:
        alert.notification_channels = req.notification_channels
    if req.notification_config is not None:
        alert.notification_config = req.notification_config

    await db.flush()
    await db.refresh(alert)
    return alert


async def delete_alert(alert_id: uuid.UUID, user_id: uuid.UUID, db: AsyncSession) -> None:
    from fastapi import HTTPException, status

    alert = await get_alert(alert_id, user_id, db)
    if alert is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Alert not found")
    await db.delete(alert)
    await db.flush()


def evaluate_condition(condition: dict[str, Any], value: float) -> bool:
    """Evaluate a simple condition dict against a numeric value."""
    op = condition.get("operator", "gt")
    threshold = float(condition.get("threshold", 0))
    return {
        "gt": value > threshold,
        "gte": value >= threshold,
        "lt": value < threshold,
        "lte": value <= threshold,
        "eq": abs(value - threshold) < 1e-9,
    }.get(op, False)
