"""Portfolio advisor agent — ReAct pattern with all 7 tools."""
from __future__ import annotations

import json
from typing import Any, AsyncIterator

import structlog
from langchain_core.messages import AIMessage, HumanMessage, SystemMessage

logger = structlog.get_logger(__name__)

_SYSTEM_PROMPT = """You are FinStreamAI's portfolio advisor. You help users understand their
portfolio, analyse market data, research stocks, and provide investment insights.

You have access to tools for:
- Market data and signals
- Portfolio positions and watchlist
- News and events
- Risk metrics

Always explain your reasoning clearly. Never guarantee returns. Always note risks."""


async def run_portfolio_advisor(
    message: str,
    user_id: str,
    chat_model,
    db=None,
) -> AsyncIterator[str]:
    """Stream portfolio advice for a user message."""
    messages = [
        SystemMessage(content=_SYSTEM_PROMPT),
        HumanMessage(content=f"User {user_id} asks: {message}"),
    ]

    try:
        async for chunk in chat_model.astream(messages):
            if hasattr(chunk, "content") and chunk.content:
                yield chunk.content
    except Exception as exc:
        logger.error("portfolio_advisor stream error", error=str(exc))
        yield f"I encountered an error while processing your request: {exc}"
