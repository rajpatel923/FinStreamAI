"""Query service: parameterized TimescaleDB queries."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

import structlog
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

logger = structlog.get_logger(__name__)

_ALLOWED_TABLES = frozenset(
    {
        "market_ticks",
        "market_bars",
        "technical_indicators",
        "sentiment_scores",
        "trading_signals",
    }
)

_INTERVAL_TO_TIMEFRAME: dict[str, str] = {
    "1m": "1min",
    "5m": "5min",
    "15m": "15min",
    "1h": "1hour",
    "1d": "1hour",
    "1min": "1min",
    "5min": "5min",
    "15min": "15min",
    "1hour": "1hour",
}

_ALLOWED_ORDER_DIRS = frozenset({"asc", "desc"})


async def query_market_data(
    db: AsyncSession,
    symbol: str,
    from_ts: datetime | None = None,
    to_ts: datetime | None = None,
    limit: int = 1000,
    cursor: str | None = None,
    hours_limit: int | None = None,
    interval: str = "1h",
) -> dict[str, Any]:
    now = datetime.now(timezone.utc)
    if from_ts is None:
        from_ts = now - timedelta(hours=hours_limit or 24)
    if to_ts is None:
        to_ts = now

    timeframe = _INTERVAL_TO_TIMEFRAME.get(interval, "1hour")

    params: dict[str, Any] = {
        "symbol": symbol.upper(),
        "timeframe": timeframe,
        "from_ts": from_ts,
        "to_ts": to_ts,
        "limit": min(limit, 10000),
    }

    cursor_clause = ""
    if cursor:
        try:
            cursor_ts = datetime.fromisoformat(cursor)
            cursor_clause = "AND time < :cursor_ts"
            params["cursor_ts"] = cursor_ts
        except ValueError:
            pass

    sql = text(
        f"""
        SELECT time AS timestamp, symbol,
               open_price AS open, high_price AS high,
               low_price AS low, close_price AS close,
               volume, vwap
        FROM market_bars
        WHERE symbol = :symbol
          AND timeframe = :timeframe
          AND time >= :from_ts
          AND time <= :to_ts
          {cursor_clause}
        ORDER BY time DESC
        LIMIT :limit
        """
    )
    result = await db.execute(sql, params)
    rows = result.mappings().all()

    next_cursor = None
    if len(rows) == params["limit"]:
        next_cursor = rows[-1]["timestamp"].isoformat()

    return {
        "symbol": symbol.upper(),
        "data": [dict(r) for r in rows],
        "cursor": next_cursor,
    }


async def query_sentiment(
    db: AsyncSession,
    symbol: str,
    from_ts: datetime | None = None,
    to_ts: datetime | None = None,
    limit: int = 500,
) -> dict[str, Any]:
    now = datetime.now(timezone.utc)
    if from_ts is None:
        from_ts = now - timedelta(hours=24)
    if to_ts is None:
        to_ts = now

    sql = text(
        """
        SELECT created_at AS timestamp, symbol, sentiment_score, sentiment_label, source
        FROM news_articles_scored
        WHERE (:symbol IS NULL OR symbol = :symbol)
          AND created_at >= :from_ts
          AND created_at <= :to_ts
        ORDER BY created_at DESC
        LIMIT :limit
        """
    )
    result = await db.execute(
        sql,
        {"symbol": symbol.upper() if symbol else None, "from_ts": from_ts, "to_ts": to_ts, "limit": limit},
    )
    rows = result.mappings().all()
    return {"data": [dict(r) for r in rows], "cursor": None}


async def run_custom_query(
    db: AsyncSession,
    table: str,
    symbol: str | None,
    from_ts: datetime | None,
    to_ts: datetime | None,
    limit: int,
    filters: dict[str, Any],
    order_by: str,
    order_dir: str,
) -> dict[str, Any]:
    if table not in _ALLOWED_TABLES:
        raise ValueError(f"Table '{table}' is not queryable")
    if order_dir.lower() not in _ALLOWED_ORDER_DIRS:
        raise ValueError("order_dir must be 'asc' or 'desc'")
    # Restrict order_by to safe column name (no injections)
    if not order_by.replace("_", "").isalnum():
        raise ValueError("order_by must be an alphanumeric column name")

    params: dict[str, Any] = {"limit": min(limit, 10000)}
    where_clauses = []

    if symbol:
        where_clauses.append("symbol = :symbol")
        params["symbol"] = symbol.upper()
    if from_ts:
        where_clauses.append("timestamp >= :from_ts")
        params["from_ts"] = from_ts
    if to_ts:
        where_clauses.append("timestamp <= :to_ts")
        params["to_ts"] = to_ts

    # Safe filter key validation
    for k, v in filters.items():
        if not k.replace("_", "").isalnum():
            continue
        where_clauses.append(f"{k} = :{k}")
        params[k] = v

    where_sql = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""
    sql = text(
        f"SELECT * FROM {table} {where_sql} ORDER BY {order_by} {order_dir} LIMIT :limit"
    )
    result = await db.execute(sql, params)
    rows = result.mappings().all()
    return {"rows": [dict(r) for r in rows], "cursor": None, "row_count": len(rows)}
