"""Pydantic schemas for watchlist and preferences endpoints."""
from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field, field_validator

VALID_RISK_TOLERANCES = {"conservative", "moderate", "aggressive"}
VALID_HORIZONS = {"short_term", "medium_term", "long_term"}
VALID_DIGEST_FREQUENCIES = {"realtime", "daily", "weekly", "never"}


class WatchlistItemCreate(BaseModel):
    symbol: str = Field(min_length=1, max_length=20)
    notes: str | None = Field(default=None, max_length=500)
    alert_on_signal: bool = True

    @field_validator("symbol")
    @classmethod
    def uppercase_symbol(cls, v: str) -> str:
        return v.upper().strip()


class WatchlistItemResponse(BaseModel):
    id: uuid.UUID
    user_id: uuid.UUID
    symbol: str
    notes: str | None
    alert_on_signal: bool
    created_at: datetime

    model_config = {"from_attributes": True}


class UserPreferenceUpdate(BaseModel):
    risk_tolerance: str | None = None
    investment_horizon: str | None = None
    preferred_sectors: list[str] | None = None
    notification_channels: dict[str, Any] | None = None
    digest_frequency: str | None = None
    auto_trading_enabled: bool | None = None
    broker_paper_trading: bool | None = None
    max_daily_loss_pct: float | None = Field(default=None, ge=0.001, le=1.0)
    max_position_size_pct: float | None = Field(default=None, ge=0.001, le=1.0)
    confirmation_threshold_usd: float | None = Field(default=None, ge=0.0)

    @field_validator("risk_tolerance")
    @classmethod
    def validate_risk_tolerance(cls, v: str | None) -> str | None:
        if v is not None and v not in VALID_RISK_TOLERANCES:
            raise ValueError(f"risk_tolerance must be one of {VALID_RISK_TOLERANCES}")
        return v

    @field_validator("investment_horizon")
    @classmethod
    def validate_horizon(cls, v: str | None) -> str | None:
        if v is not None and v not in VALID_HORIZONS:
            raise ValueError(f"investment_horizon must be one of {VALID_HORIZONS}")
        return v

    @field_validator("digest_frequency")
    @classmethod
    def validate_digest(cls, v: str | None) -> str | None:
        if v is not None and v not in VALID_DIGEST_FREQUENCIES:
            raise ValueError(f"digest_frequency must be one of {VALID_DIGEST_FREQUENCIES}")
        return v


class UserPreferenceResponse(BaseModel):
    id: uuid.UUID
    user_id: uuid.UUID
    risk_tolerance: str
    investment_horizon: str
    preferred_sectors: list[str]
    notification_channels: dict[str, Any]
    digest_frequency: str
    auto_trading_enabled: bool
    broker_paper_trading: bool
    max_daily_loss_pct: float
    max_position_size_pct: float
    confirmation_threshold_usd: float
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}
