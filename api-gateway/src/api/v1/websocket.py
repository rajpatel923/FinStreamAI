"""WebSocket streaming endpoint."""
from __future__ import annotations

import asyncio
import json

import redis.asyncio as aioredis
import structlog
from fastapi import APIRouter, Depends, WebSocket, WebSocketDisconnect, WebSocketException, status
from jose import JWTError
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.config import settings
from src.core.database import get_db, AsyncPostgresSession
from src.core.security import decode_access_token
from src.core.dependencies import get_redis
from src.models.user import User
from src.services.ws_manager import ConnectionManager

logger = structlog.get_logger(__name__)
router = APIRouter(tags=["websocket"])

# Shared manager — injected from app.state in main.py
_manager: ConnectionManager | None = None


def set_manager(manager: ConnectionManager) -> None:
    global _manager
    _manager = manager


async def _authenticate_ws(token: str) -> User:
    """Validate JWT and return User, raise WebSocketException on failure."""
    try:
        payload = decode_access_token(token)
    except JWTError as e:
        raise WebSocketException(code=status.WS_1008_POLICY_VIOLATION, reason="Invalid token") from e

    user_id = payload.get("sub")
    if not user_id:
        raise WebSocketException(code=status.WS_1008_POLICY_VIOLATION, reason="Invalid token")

    async with AsyncPostgresSession() as db:
        result = await db.execute(select(User).where(User.id == user_id))
        user = result.scalar_one_or_none()

    if user is None or not user.is_active:
        raise WebSocketException(code=status.WS_1008_POLICY_VIOLATION, reason="User not found")
    return user


@router.websocket("/ws/stream")
async def websocket_stream(
    websocket: WebSocket,
    token: str,
):
    manager = _manager
    if manager is None:
        await websocket.close(code=status.WS_1011_INTERNAL_ERROR)
        return

    user = await _authenticate_ws(token)
    user_id = str(user.id)

    await manager.connect(user_id, websocket)

    # Enforce free tier: 1 symbol only
    redis = aioredis.from_url(settings.redis_url, decode_responses=True)
    max_symbols = 1 if user.role == "free_user" else 999

    # Send welcome
    await websocket.send_json({"type": "connected", "user_id": user_id, "role": user.role})

    ping_task = asyncio.create_task(_ping_loop(websocket, user_id, manager))

    try:
        while True:
            try:
                raw = await asyncio.wait_for(websocket.receive_text(), timeout=35.0)
            except asyncio.TimeoutError:
                # Client didn't pong — close
                break

            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                continue

            msg_type = msg.get("type") or msg.get("action")
            if msg_type == "pong":
                continue
            elif msg_type == "subscribe":
                symbols = msg.get("symbols", [])[:max_symbols]
                channels = msg.get("channels", ["market_data"])
                # Store subscription in Redis
                if symbols:
                    await redis.delete(f"ws:subs:{user_id}:symbols")
                    await redis.sadd(f"ws:subs:{user_id}:symbols", *symbols)
                    await redis.expire(f"ws:subs:{user_id}:symbols", 3600)
                await websocket.send_json(
                    {"type": "subscribed", "symbols": symbols, "channels": channels}
                )
            elif msg_type == "unsubscribe":
                await redis.delete(f"ws:subs:{user_id}:symbols")
                await websocket.send_json({"type": "unsubscribed"})

    except WebSocketDisconnect:
        pass
    except Exception as exc:
        logger.error("WebSocket error", user_id=user_id, error=str(exc))
    finally:
        ping_task.cancel()
        await manager.disconnect(user_id, websocket)
        await redis.aclose()


async def _ping_loop(ws: WebSocket, user_id: str, manager: ConnectionManager) -> None:
    while True:
        await asyncio.sleep(30)
        try:
            await ws.send_json({"type": "ping"})
        except Exception:
            break
