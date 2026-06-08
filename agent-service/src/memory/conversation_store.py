"""Conversation and message persistence."""
from __future__ import annotations

import uuid
from typing import Any

import structlog
from fastapi import HTTPException, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.models.conversation import AgentConversation, AgentMessage

logger = structlog.get_logger(__name__)


async def get_or_create_conversation(
    user_id: uuid.UUID,
    thread_id: str,
    db: AsyncSession,
    agent_type: str = "supervisor",
    title: str | None = None,
) -> AgentConversation:
    result = await db.execute(
        select(AgentConversation).where(AgentConversation.thread_id == thread_id)
    )
    conv = result.scalar_one_or_none()
    if conv is None:
        conv = AgentConversation(
            user_id=user_id,
            thread_id=thread_id,
            agent_type=agent_type,
            title=title,
        )
        db.add(conv)
        await db.flush()
        await db.refresh(conv)
    return conv


async def append_message(
    conversation_id: uuid.UUID,
    role: str,
    content: str,
    db: AsyncSession,
    tool_calls: Any | None = None,
) -> AgentMessage:
    msg = AgentMessage(
        conversation_id=conversation_id,
        role=role,
        content=content,
        tool_calls=tool_calls,
    )
    db.add(msg)
    await db.flush()
    await db.refresh(msg)
    return msg


async def list_messages(
    conversation_id: uuid.UUID,
    user_id: uuid.UUID,
    db: AsyncSession,
) -> list[AgentMessage]:
    # Verify ownership
    result = await db.execute(
        select(AgentConversation).where(
            AgentConversation.id == conversation_id,
            AgentConversation.user_id == user_id,
        )
    )
    conv = result.scalar_one_or_none()
    if conv is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Session not found")

    result = await db.execute(
        select(AgentMessage)
        .where(AgentMessage.conversation_id == conversation_id)
        .order_by(AgentMessage.created_at.asc())
    )
    return result.scalars().all()


async def list_conversations(
    user_id: uuid.UUID,
    db: AsyncSession,
) -> list[AgentConversation]:
    result = await db.execute(
        select(AgentConversation)
        .where(AgentConversation.user_id == user_id)
        .order_by(AgentConversation.updated_at.desc())
    )
    return result.scalars().all()


async def delete_conversation(
    conversation_id: uuid.UUID,
    user_id: uuid.UUID,
    db: AsyncSession,
) -> None:
    result = await db.execute(
        select(AgentConversation).where(
            AgentConversation.id == conversation_id,
            AgentConversation.user_id == user_id,
        )
    )
    conv = result.scalar_one_or_none()
    if conv is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Session not found")
    await db.delete(conv)
    await db.flush()
