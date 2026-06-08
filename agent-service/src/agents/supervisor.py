"""Supervisor agent — classifies intent and routes to sub-agents."""
from __future__ import annotations

import json
from typing import Any, AsyncIterator, TypedDict

import structlog
from langchain_core.messages import AIMessage, HumanMessage, SystemMessage

logger = structlog.get_logger(__name__)

_CLASSIFY_PROMPT = """Classify the user's intent as either 'portfolio_advice' or 'trade_request'.
Return JSON only: {"intent": "portfolio_advice"} or {"intent": "trade_request"}"""


class AgentState(TypedDict):
    messages: list
    intent: str
    user_id: str
    user_role: str


def build_supervisor(chat_model, checkpointer=None):
    """Build a LangGraph StateGraph supervisor."""
    try:
        from langgraph.graph import StateGraph, END

        workflow = StateGraph(AgentState)

        async def classify_intent(state: AgentState) -> AgentState:
            last_msg = state["messages"][-1] if state["messages"] else None
            content = last_msg.content if hasattr(last_msg, "content") else str(last_msg)
            try:
                response = await chat_model.ainvoke(
                    [
                        SystemMessage(content=_CLASSIFY_PROMPT),
                        HumanMessage(content=content),
                    ]
                )
                parsed = json.loads(response.content)
                intent = parsed.get("intent", "portfolio_advice")
            except Exception:
                intent = "portfolio_advice"
            return {**state, "intent": intent}

        async def portfolio_advisor_node(state: AgentState) -> AgentState:
            from src.agents.portfolio_advisor import run_portfolio_advisor

            last_msg = state["messages"][-1] if state["messages"] else None
            content = last_msg.content if hasattr(last_msg, "content") else str(last_msg)

            chunks = []
            async for chunk in run_portfolio_advisor(content, state["user_id"], chat_model):
                chunks.append(chunk)

            response_text = "".join(chunks)
            new_messages = list(state["messages"]) + [AIMessage(content=response_text)]
            return {**state, "messages": new_messages}

        async def trade_executor_node(state: AgentState) -> AgentState:
            last_msg = state["messages"][-1] if state["messages"] else None
            content = last_msg.content if hasattr(last_msg, "content") else str(last_msg)
            response = f"Trade requests must be submitted via the /trading/orders endpoint. Message: {content}"
            new_messages = list(state["messages"]) + [AIMessage(content=response)]
            return {**state, "messages": new_messages}

        def route_intent(state: AgentState) -> str:
            return "portfolio_advisor" if state.get("intent") != "trade_request" else "trade_executor"

        workflow.add_node("classify_intent", classify_intent)
        workflow.add_node("portfolio_advisor", portfolio_advisor_node)
        workflow.add_node("trade_executor", trade_executor_node)

        workflow.set_entry_point("classify_intent")
        workflow.add_conditional_edges("classify_intent", route_intent)
        workflow.add_edge("portfolio_advisor", END)
        workflow.add_edge("trade_executor", END)

        if checkpointer is not None:
            return workflow.compile(checkpointer=checkpointer)
        return workflow.compile()

    except ImportError:
        logger.warning("langgraph not installed — supervisor disabled")
        return None


async def stream_supervisor(
    supervisor,
    message: str,
    user_id: str,
    user_role: str,
    thread_id: str,
) -> AsyncIterator[str]:
    """Stream responses from the supervisor graph."""
    if supervisor is None:
        yield f"Agent not available. Message: {message}"
        return

    initial_state: AgentState = {
        "messages": [HumanMessage(content=message)],
        "intent": "",
        "user_id": user_id,
        "user_role": user_role,
    }
    config = {"configurable": {"thread_id": thread_id}}

    try:
        async for event in supervisor.astream(initial_state, config=config):
            for node_name, node_output in event.items():
                if node_name in ("portfolio_advisor", "trade_executor"):
                    msgs = node_output.get("messages", [])
                    if msgs:
                        last = msgs[-1]
                        content = last.content if hasattr(last, "content") else str(last)
                        yield content
    except Exception as exc:
        logger.error("supervisor stream error", error=str(exc))
        yield f"Error: {exc}"
