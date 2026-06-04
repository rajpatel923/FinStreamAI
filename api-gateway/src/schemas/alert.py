"""Pydantic schemas for alert endpoints."""
from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field, field_validator

from src.core.security import validate_webhook_url

VALID_ALERT_TYPES = {"price", "sentiment", "event", "anomaly", "custom"}
VALID_CHANNELS = {"email", "slack", "webhook", "sms"}


class AlertCreate(BaseModel):
    name: str = Field(min_length=1, max_length=255)
    alert_type: str
    symbol: str | None = Field(default=None, max_length=20)
    condition: dict[str, Any]
    notification_channels: list[str]
    notification_config: dict[str, Any] = Field(default_factory=dict)

    @field_validator("alert_type")
    @classmethod
    def validate_alert_type(cls, v: str) -> str:
        if v not in VALID_ALERT_TYPES:
            raise ValueError(f"alert_type must be one of {VALID_ALERT_TYPES}")
        return v

    @field_validator("notification_channels")
    @classmethod
    def validate_channels(cls, v: list[str]) -> list[str]:
        invalid = set(v) - VALID_CHANNELS
        if invalid:
            raise ValueError(f"Invalid channels: {invalid}")
        return v

    @field_validator("notification_config")
    @classmethod
    def validate_webhook_ssrf(cls, v: dict, info: Any) -> dict:
        webhook_url = v.get("webhook_url")
        if webhook_url and not validate_webhook_url(webhook_url):
            raise ValueError("webhook_url targets a private/internal address (SSRF blocked)")
        slack_url = v.get("slack_url")
        if slack_url and not validate_webhook_url(slack_url):
            raise ValueError("slack_url targets a private/internal address (SSRF blocked)")
        return v


class AlertUpdate(BaseModel):
    name: str | None = Field(default=None, min_length=1, max_length=255)
    is_active: bool | None = None
    condition: dict[str, Any] | None = None
    notification_channels: list[str] | None = None
    notification_config: dict[str, Any] | None = None

    @field_validator("notification_config")
    @classmethod
    def validate_webhook_ssrf(cls, v: dict | None) -> dict | None:
        if v is None:
            return v
        webhook_url = v.get("webhook_url")
        if webhook_url and not validate_webhook_url(webhook_url):
            raise ValueError("webhook_url targets a private/internal address (SSRF blocked)")
        return v


class AlertResponse(BaseModel):
    id: uuid.UUID
    name: str
    alert_type: str
    symbol: str | None
    condition: dict[str, Any]
    notification_channels: list[str]
    notification_config: dict[str, Any]
    is_active: bool
    last_triggered_at: datetime | None
    trigger_count: int
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}
