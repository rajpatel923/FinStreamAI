"""Internal cross-service endpoints (not in OpenAPI docs)."""
from __future__ import annotations

from typing import Any

import structlog
from fastapi import APIRouter, HTTPException, Request, status
from pydantic import BaseModel

from src.core.config import settings

logger = structlog.get_logger(__name__)

router = APIRouter(prefix="/internal", include_in_schema=False)


class WsPushRequest(BaseModel):
    user_id: str
    message: dict[str, Any]


@router.post("/ws/push", status_code=status.HTTP_204_NO_CONTENT)
async def ws_push(req: WsPushRequest, request: Request):
    secret = request.headers.get("X-Internal-Secret", "")
    if not settings.INTERNAL_PUSH_SECRET or secret != settings.INTERNAL_PUSH_SECRET:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Forbidden")

    manager = getattr(request.app.state, "ws_manager", None)
    if manager is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="WebSocket manager not initialised",
        )

    await manager.broadcast(req.user_id, req.message)
