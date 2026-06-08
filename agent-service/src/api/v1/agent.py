"""Agent chat and session management endpoints."""
from __future__ import annotations

import uuid
from typing import Annotated, AsyncIterator

import structlog
from fastapi import APIRouter, Depends, HTTPException, Request, status
from fastapi.responses import StreamingResponse
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.database import get_db
from src.core.dependencies import get_current_user
from src.memory import conversation_store
from src.models.user import User
from src.schemas.agent import ChatRequest, MessageResponse, SessionResponse

logger = structlog.get_logger(__name__)
router = APIRouter(prefix="/agent", tags=["agent"])


@router.post("/chat")
async def chat(
    req: ChatRequest,
    request: Request,
    user: Annotated[User, Depends(get_current_user)],
    db: Annotated[AsyncSession, Depends(get_db)],
):
    """SSE streaming chat endpoint."""
    thread_id = req.session_id or str(uuid.uuid4())

    conv = await conversation_store.get_or_create_conversation(
        user.id, thread_id, db,
        title=req.message[:50] if not req.session_id else None,
    )
    await conversation_store.append_message(conv.id, "human", req.message, db)

    supervisor = getattr(request.app.state, "supervisor", None)

    async def _generate() -> AsyncIterator[str]:
        from src.agents.supervisor import stream_supervisor

        full_response = []
        async for chunk in stream_supervisor(supervisor, req.message, str(user.id), user.role, thread_id):
            full_response.append(chunk)
            yield f"data: {chunk}\n\n"

        # Persist assistant response
        if full_response:
            response_text = "".join(full_response)
            await conversation_store.append_message(conv.id, "assistant", response_text, db)
            await db.commit()

    try:
        from sse_starlette.sse import EventSourceResponse
        return EventSourceResponse(_generate())
    except ImportError:
        # Fallback: collect all chunks and return as plain text
        from src.agents.supervisor import stream_supervisor
        chunks = []
        async for chunk in stream_supervisor(supervisor, req.message, str(user.id), user.role, thread_id):
            chunks.append(chunk)
        return {"response": "".join(chunks), "session_id": thread_id}


@router.get("/sessions", response_model=list[SessionResponse])
async def list_sessions(
    user: Annotated[User, Depends(get_current_user)],
    db: Annotated[AsyncSession, Depends(get_db)],
):
    convs = await conversation_store.list_conversations(user.id, db)
    return convs


@router.get("/sessions/{session_id}/messages", response_model=list[MessageResponse])
async def get_messages(
    session_id: uuid.UUID,
    user: Annotated[User, Depends(get_current_user)],
    db: Annotated[AsyncSession, Depends(get_db)],
):
    return await conversation_store.list_messages(session_id, user.id, db)


@router.delete("/sessions/{session_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_session(
    session_id: uuid.UUID,
    user: Annotated[User, Depends(get_current_user)],
    db: Annotated[AsyncSession, Depends(get_db)],
):
    await conversation_store.delete_conversation(session_id, user.id, db)
