"""TradeOrder ORM model."""
from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import Boolean, Float, ForeignKey, String, Text, Uuid
from sqlalchemy import TIMESTAMP as _TS
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func

from src.core.database import Base


class TradeOrder(Base):
    __tablename__ = "trade_orders"

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    symbol: Mapped[str] = mapped_column(String(20), nullable=False, index=True)
    side: Mapped[str] = mapped_column(String(10), nullable=False)
    qty: Mapped[float] = mapped_column(Float, nullable=False)
    order_type: Mapped[str] = mapped_column(String(20), nullable=False, default="market")
    status: Mapped[str] = mapped_column(String(30), nullable=False, default="pending")
    broker_order_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    agent_reasoning: Mapped[str | None] = mapped_column(Text, nullable=True)
    risk_score: Mapped[float | None] = mapped_column(Float, nullable=True)
    is_paper: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    created_at: Mapped[datetime] = mapped_column(
        _TS(timezone=True), nullable=False, server_default=func.now()
    )
    filled_at: Mapped[datetime | None] = mapped_column(_TS(timezone=True), nullable=True)
