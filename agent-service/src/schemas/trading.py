"""Pydantic schemas for trading endpoints."""
from __future__ import annotations

import uuid
from datetime import datetime

from pydantic import BaseModel, Field


VALID_SIDES = {"buy", "sell"}
VALID_ORDER_TYPES = {"market", "limit", "stop", "stop_limit"}


class OrderRequest(BaseModel):
    symbol: str = Field(min_length=1, max_length=20)
    side: str
    qty: float = Field(gt=0)
    order_type: str = "market"
    limit_price: float | None = None

    class Config:
        use_enum_values = True


class OrderResponse(BaseModel):
    id: uuid.UUID
    user_id: uuid.UUID
    symbol: str
    side: str
    qty: float
    order_type: str
    status: str
    broker_order_id: str | None
    agent_reasoning: str | None
    risk_score: float | None
    is_paper: bool
    created_at: datetime
    filled_at: datetime | None

    model_config = {"from_attributes": True}


class KillSwitchResponse(BaseModel):
    status: str
    user_id: str
    message: str
