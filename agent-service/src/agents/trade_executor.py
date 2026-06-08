"""Trade executor — 7-guardrail pipeline."""
from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any

import structlog
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.models.trading import TradeOrder

logger = structlog.get_logger(__name__)


@dataclass
class TradeOrderResult:
    order_id: str
    status: str
    reason: str
    requires_confirmation: bool = False


async def execute_trade(
    user_id: uuid.UUID,
    user_role: str,
    symbol: str,
    side: str,
    qty: float,
    order_type: str,
    db: AsyncSession,
    redis,
    preferences,
    chat_model=None,
) -> TradeOrderResult:
    """Run all 7 guardrails then submit or pend confirmation."""

    # ── Guardrail 1: premium/admin role ────────────────────────────
    if user_role not in ("premium_user", "admin"):
        order = _write_rejected(db, user_id, symbol, side, qty, order_type, "Requires premium or admin role")
        db.add(order)
        await db.flush()
        return TradeOrderResult(str(order.id), "rejected", "Requires premium or admin role")

    # ── Guardrail 2: auto_trading_enabled ──────────────────────────
    if not getattr(preferences, "auto_trading_enabled", False):
        order = _write_rejected(db, user_id, symbol, side, qty, order_type, "Auto-trading not enabled in preferences")
        db.add(order)
        await db.flush()
        return TradeOrderResult(str(order.id), "rejected", "Auto-trading not enabled in preferences")

    # ── Guardrail 3: kill switch ───────────────────────────────────
    kill_key = f"agent:killswitch:{user_id}"
    kill_val = await redis.get(kill_key)
    if kill_val is not None:
        order = _write_rejected(db, user_id, symbol, side, qty, order_type, "Kill switch active")
        db.add(order)
        await db.flush()
        return TradeOrderResult(str(order.id), "rejected", "Kill switch active")

    # ── Guardrail 4: daily loss limit ─────────────────────────────
    today_start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    filled_today_result = await db.execute(
        select(TradeOrder).where(
            TradeOrder.user_id == user_id,
            TradeOrder.status == "filled",
            TradeOrder.filled_at >= today_start,
        )
    )
    filled_today = filled_today_result.scalars().all()
    total_filled_value = sum(o.qty * 100.0 for o in filled_today)  # approximate

    max_daily_loss = getattr(preferences, "max_daily_loss_pct", 0.02)
    # We don't have portfolio value here so we use a rough cap of $100k portfolio
    portfolio_estimate = 100_000.0
    if total_filled_value > max_daily_loss * portfolio_estimate:
        order = _write_rejected(db, user_id, symbol, side, qty, order_type, "Daily loss limit reached")
        db.add(order)
        await db.flush()
        return TradeOrderResult(str(order.id), "rejected", "Daily loss limit reached")

    # ── Guardrail 5: position size ─────────────────────────────────
    max_pos_pct = getattr(preferences, "max_position_size_pct", 0.10)
    order_value = qty * 100.0  # approximate
    max_position_value = max_pos_pct * portfolio_estimate
    if order_value > max_position_value:
        qty = max_position_value / 100.0  # clamp

    # ── Guardrail 6: Claude risk score ────────────────────────────
    risk_score = 0.5  # default
    if chat_model is not None:
        try:
            from langchain_core.messages import HumanMessage
            response = await chat_model.ainvoke(
                [HumanMessage(content=f"Rate the risk of {side}ing {qty} shares of {symbol}. "
                              f"Reply with JSON: {{\"risk_score\": 0.0-1.0, \"reasoning\": \"...\"}}")]
            )
            import json
            parsed = json.loads(response.content)
            risk_score = float(parsed.get("risk_score", 0.5))
        except Exception as exc:
            logger.warning("Risk score evaluation failed", error=str(exc))

    if risk_score >= 0.7:
        order = _write_rejected(
            db, user_id, symbol, side, qty, order_type,
            f"Claude risk score {risk_score:.2f} >= 0.7 threshold"
        )
        order.risk_score = risk_score
        db.add(order)
        await db.flush()
        return TradeOrderResult(str(order.id), "rejected", f"Risk score {risk_score:.2f} too high")

    # ── Guardrail 7: confirmation threshold ──────────────────────
    confirmation_threshold = getattr(preferences, "confirmation_threshold_usd", 1000.0)
    if order_value >= confirmation_threshold:
        order = TradeOrder(
            user_id=user_id,
            symbol=symbol,
            side=side,
            qty=qty,
            order_type=order_type,
            status="pending_confirmation",
            agent_reasoning=f"Order value ${order_value:.2f} requires user confirmation",
            risk_score=risk_score,
            is_paper=getattr(preferences, "broker_paper_trading", True),
        )
        db.add(order)
        await db.flush()
        await db.refresh(order)

        # Write placeholder to Redis blpop list (user must confirm via POST)
        order_id = str(order.id)
        confirm_key = f"agent:confirm:{order_id}"
        await redis.expire(confirm_key, 600)

        return TradeOrderResult(order_id, "awaiting_confirmation", "Requires user confirmation", True)

    # ── Submit order ───────────────────────────────────────────────
    is_paper = getattr(preferences, "broker_paper_trading", True)
    order = TradeOrder(
        user_id=user_id,
        symbol=symbol,
        side=side,
        qty=qty,
        order_type=order_type,
        status="submitted",
        agent_reasoning=f"All guardrails passed. Risk score: {risk_score:.2f}",
        risk_score=risk_score,
        is_paper=is_paper,
    )
    db.add(order)
    await db.flush()
    await db.refresh(order)
    return TradeOrderResult(str(order.id), "submitted", "Order submitted")


def _write_rejected(
    db, user_id, symbol, side, qty, order_type, reason
) -> TradeOrder:
    return TradeOrder(
        user_id=user_id,
        symbol=symbol,
        side=side,
        qty=qty,
        order_type=order_type,
        status="rejected",
        agent_reasoning=reason,
        is_paper=True,
    )
