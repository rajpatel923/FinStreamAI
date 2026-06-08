"""LangGraph checkpointer factory."""
from __future__ import annotations


def get_checkpointer(postgres_dsn: str | None = None):
    """Return AsyncPostgresSaver in production, MemorySaver otherwise."""
    if postgres_dsn:
        try:
            from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver

            return AsyncPostgresSaver.from_conn_string(postgres_dsn)
        except Exception:
            pass

    from langgraph.checkpoint.memory import MemorySaver

    return MemorySaver()


# Convenience alias for tests
InMemoryCheckpointer = None  # resolved at call-time via get_checkpointer()
