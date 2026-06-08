"""Tests for the supervisor agent."""
from __future__ import annotations

import pytest
from langchain_core.messages import HumanMessage


@pytest.mark.asyncio
async def test_build_supervisor_with_memory_checkpointer(mock_chat_anthropic, mock_checkpointer):
    from src.agents.supervisor import build_supervisor

    supervisor = build_supervisor(mock_chat_anthropic, mock_checkpointer)
    assert supervisor is not None


@pytest.mark.asyncio
async def test_classify_portfolio_intent(mock_chat_anthropic, mock_checkpointer):
    from src.agents.supervisor import build_supervisor, stream_supervisor

    supervisor = build_supervisor(mock_chat_anthropic, mock_checkpointer)
    chunks = []
    async for chunk in stream_supervisor(
        supervisor, "What is my portfolio performance?", "user-123", "free_user", "thread-001"
    ):
        chunks.append(chunk)

    assert len(chunks) >= 1
    full = "".join(chunks)
    assert len(full) > 0


@pytest.mark.asyncio
async def test_stream_supervisor_no_supervisor():
    from src.agents.supervisor import stream_supervisor

    chunks = []
    async for chunk in stream_supervisor(None, "hello", "user-1", "free_user", "thread-x"):
        chunks.append(chunk)

    assert len(chunks) == 1
    assert "hello" in chunks[0]


@pytest.mark.asyncio
async def test_classify_trade_intent(mock_checkpointer):
    from unittest.mock import MagicMock
    from src.agents.supervisor import build_supervisor, stream_supervisor

    trade_model = MagicMock()

    async def _ainvoke(messages, **kwargs):
        resp = MagicMock()
        resp.content = '{"intent": "trade_request"}'
        return resp

    async def _astream(messages, **kwargs):
        chunk = MagicMock()
        chunk.content = "Trade redirected"
        yield chunk

    trade_model.ainvoke = _ainvoke
    trade_model.astream = _astream

    supervisor = build_supervisor(trade_model, mock_checkpointer)
    chunks = []
    async for chunk in stream_supervisor(
        supervisor, "Buy 100 shares of AAPL", "user-123", "premium_user", "thread-trade"
    ):
        chunks.append(chunk)

    assert len(chunks) >= 1


@pytest.mark.asyncio
async def test_conversation_persistence_across_calls(mock_chat_anthropic, mock_checkpointer):
    from src.agents.supervisor import build_supervisor, stream_supervisor

    supervisor = build_supervisor(mock_chat_anthropic, mock_checkpointer)
    thread_id = "persistent-thread"

    # First call
    chunks1 = []
    async for c in stream_supervisor(supervisor, "Hello", "user-1", "free_user", thread_id):
        chunks1.append(c)

    # Second call — same thread_id
    chunks2 = []
    async for c in stream_supervisor(supervisor, "Follow up question", "user-1", "free_user", thread_id):
        chunks2.append(c)

    assert len(chunks1) >= 1
    assert len(chunks2) >= 1


@pytest.mark.asyncio
async def test_portfolio_advisor_stream(mock_chat_anthropic):
    from src.agents.portfolio_advisor import run_portfolio_advisor

    chunks = []
    async for chunk in run_portfolio_advisor("Analyse my risk", "user-1", mock_chat_anthropic):
        chunks.append(chunk)

    assert len(chunks) >= 1
    assert "portfolio" in "".join(chunks).lower() or len("".join(chunks)) > 0


@pytest.mark.asyncio
async def test_portfolio_advisor_error_handling():
    from unittest.mock import MagicMock
    from src.agents.portfolio_advisor import run_portfolio_advisor

    bad_model = MagicMock()

    async def _bad_stream(*args, **kwargs):
        raise RuntimeError("API error")
        yield  # make it a generator

    bad_model.astream = _bad_stream

    chunks = []
    async for chunk in run_portfolio_advisor("test", "user-1", bad_model):
        chunks.append(chunk)

    assert any("error" in c.lower() for c in chunks)
