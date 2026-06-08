"""ORM models: WatchlistItem, UserPreference."""
from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import Boolean, ForeignKey, Float, Integer, String, Text, Uuid
from sqlalchemy import TIMESTAMP as _TS
from sqlalchemy import JSON
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func

from src.core.database import Base


class WatchlistItem(Base):
    __tablename__ = "watchlist_items"

    id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    user_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    symbol: Mapped[str] = mapped_column(String(20), nullable=False)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    alert_on_signal: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    created_at: Mapped[datetime] = mapped_column(
        _TS(timezone=True), nullable=False, server_default=func.now()
    )


class UserPreference(Base):
    __tablename__ = "user_preferences"

    id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    user_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
        unique=True,
        index=True,
    )
    risk_tolerance: Mapped[str] = mapped_column(
        String(20), nullable=False, default="moderate"
    )
    investment_horizon: Mapped[str] = mapped_column(
        String(20), nullable=False, default="medium_term"
    )
    preferred_sectors: Mapped[list] = mapped_column(JSON, nullable=False, default=list)
    notification_channels: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)
    digest_frequency: Mapped[str] = mapped_column(
        String(20), nullable=False, default="daily"
    )
    auto_trading_enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    broker_paper_trading: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    broker_api_key_encrypted: Mapped[str | None] = mapped_column(Text, nullable=True)
    max_daily_loss_pct: Mapped[float] = mapped_column(Float, nullable=False, default=0.02)
    max_position_size_pct: Mapped[float] = mapped_column(Float, nullable=False, default=0.10)
    confirmation_threshold_usd: Mapped[float] = mapped_column(Float, nullable=False, default=1000.0)
    created_at: Mapped[datetime] = mapped_column(
        _TS(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        _TS(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )
