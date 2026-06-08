"""Pydantic schemas for portfolio endpoints."""
from __future__ import annotations

import uuid
from datetime import datetime

from pydantic import BaseModel, Field


class PositionCreate(BaseModel):
    symbol: str = Field(min_length=1, max_length=20)
    quantity: float = Field(gt=0)
    avg_cost_basis: float = Field(gt=0)
    source: str = "manual"

    class Config:
        use_enum_values = True


class PositionResponse(BaseModel):
    id: uuid.UUID
    user_id: uuid.UUID
    symbol: str
    quantity: float
    avg_cost_basis: float
    is_open: bool
    source: str
    opened_at: datetime
    closed_at: datetime | None

    model_config = {"from_attributes": True}


class PortfolioSummary(BaseModel):
    total_cost_basis: float
    total_market_value: float
    unrealized_pnl: float
    positions: list[PositionResponse]
