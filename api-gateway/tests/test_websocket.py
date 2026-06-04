"""Tests for WebSocket endpoint."""
from __future__ import annotations

import json
import uuid
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from src.core.security import create_access_token
from src.main import app
from src.models.user import User
from src.services.ws_manager import ConnectionManager
from tests.conftest import make_token


def _make_ws_client():
    return TestClient(app)


@pytest.mark.asyncio
async def test_ws_invalid_token():
    """WebSocket with bad JWT raises WebSocketException before DB is reached."""
    from src.api.v1.websocket import _authenticate_ws
    from fastapi import WebSocketException
    with pytest.raises(WebSocketException):
        await _authenticate_ws("completely.invalid.jwt")


@pytest.mark.asyncio
async def test_connection_manager_connect_disconnect():
    manager = ConnectionManager()
    ws = AsyncMock()
    ws.accept = AsyncMock()
    ws.send_text = AsyncMock()

    await manager.connect("user1", ws)
    assert "user1" in manager.active_user_ids()

    await manager.disconnect("user1", ws)
    assert "user1" not in manager.active_user_ids()


@pytest.mark.asyncio
async def test_connection_manager_broadcast():
    manager = ConnectionManager()
    ws = AsyncMock()
    ws.accept = AsyncMock()
    ws.send_text = AsyncMock()

    await manager.connect("user1", ws)
    await manager.broadcast("user1", {"type": "test", "data": "hello"})

    ws.send_text.assert_called_once()
    sent = json.loads(ws.send_text.call_args[0][0])
    assert sent["type"] == "test"


@pytest.mark.asyncio
async def test_connection_manager_broadcast_all():
    manager = ConnectionManager()
    ws1, ws2 = AsyncMock(), AsyncMock()
    ws1.accept = ws2.accept = AsyncMock()
    ws1.send_text = ws2.send_text = AsyncMock()

    await manager.connect("user1", ws1)
    await manager.connect("user2", ws2)
    await manager.broadcast_all({"type": "global"})

    assert ws1.send_text.called
    assert ws2.send_text.called


@pytest.mark.asyncio
async def test_ws_authenticate_invalid_token():
    from src.api.v1.websocket import _authenticate_ws
    from fastapi import WebSocketException

    with pytest.raises(WebSocketException):
        await _authenticate_ws("bad_token")
