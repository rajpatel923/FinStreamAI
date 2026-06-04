"""Pydantic schemas for user endpoints."""
from __future__ import annotations

import uuid
from datetime import datetime

from pydantic import BaseModel, EmailStr, Field


class UserResponse(BaseModel):
    id: uuid.UUID
    email: EmailStr
    full_name: str | None
    role: str
    is_active: bool
    oauth_provider: str | None
    created_at: datetime

    model_config = {"from_attributes": True}


class UpdateProfileRequest(BaseModel):
    full_name: str | None = Field(default=None, max_length=255)


class ApiKeyCreateRequest(BaseModel):
    name: str = Field(min_length=1, max_length=100)
    expires_days: int | None = Field(default=None, ge=1, le=365)


class ApiKeyResponse(BaseModel):
    id: uuid.UUID
    key_prefix: str
    name: str
    is_active: bool
    last_used_at: datetime | None
    expires_at: datetime | None
    created_at: datetime
    # full_key only returned on creation
    full_key: str | None = None

    model_config = {"from_attributes": True}
