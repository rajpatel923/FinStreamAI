"""Unified cross-database query interface."""
from __future__ import annotations

import concurrent.futures
from dataclasses import dataclass, field
from typing import Any

import psycopg2
import psycopg2.extras
import structlog

logger = structlog.get_logger(__name__)

_QUERY_TIMEOUT_S = 5


@dataclass
class QuerySpec:
    sources: list[str]  # e.g. ["timescale", "neo4j", "redis", "postgres"]
    filters: dict[str, Any] = field(default_factory=dict)
    limit: int = 100


@dataclass
class QueryResult:
    source: str
    data: list[dict[str, Any]]
    error: str | None = None


class UnifiedQuery:
    """Routes queries to TimescaleDB, Neo4j, Redis, and Postgres in parallel."""

    def __init__(
        self,
        timescale_dsn: str | None = None,
        postgres_dsn: str | None = None,
        neo4j_client=None,
        redis_cache=None,
    ) -> None:
        self._timescale_dsn = timescale_dsn
        self._postgres_dsn = postgres_dsn
        self._neo4j = neo4j_client
        self._redis = redis_cache

    # ------------------------------------------------------------------
    # Execute
    # ------------------------------------------------------------------
    def execute(self, query_spec: QuerySpec) -> dict[str, QueryResult]:
        """Fan out across sources in parallel, collect and return results."""
        source_fns = {
            "timescale": self._query_timescale,
            "postgres": self._query_postgres,
            "neo4j": self._query_neo4j,
            "redis": self._query_redis,
        }

        results: dict[str, QueryResult] = {}
        requested = [s for s in query_spec.sources if s in source_fns]

        with concurrent.futures.ThreadPoolExecutor(max_workers=len(requested) or 1) as pool:
            futures = {
                pool.submit(source_fns[src], query_spec): src
                for src in requested
            }
            done, _ = concurrent.futures.wait(futures, timeout=_QUERY_TIMEOUT_S)

            for future in futures:
                src = futures[future]
                if future in done:
                    try:
                        results[src] = future.result()
                    except Exception as exc:
                        results[src] = QueryResult(source=src, data=[], error=str(exc))
                else:
                    future.cancel()
                    results[src] = QueryResult(source=src, data=[], error="timeout")

        return results

    # ------------------------------------------------------------------
    # Source adapters
    # ------------------------------------------------------------------
    def _query_timescale(self, spec: QuerySpec) -> QueryResult:
        if not self._timescale_dsn:
            return QueryResult(source="timescale", data=[], error="not configured")
        try:
            conn = psycopg2.connect(self._timescale_dsn)
            conn.autocommit = True
            symbol = spec.filters.get("symbol")
            limit = spec.limit

            sql = "SELECT * FROM market_ticks"
            params: list[Any] = []
            if symbol:
                sql += " WHERE symbol = %s"
                params.append(symbol)
            sql += f" ORDER BY time DESC LIMIT {limit}"

            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(sql, params)
                rows = [dict(r) for r in cur.fetchall()]
            conn.close()
            return QueryResult(source="timescale", data=rows)
        except Exception as exc:
            return QueryResult(source="timescale", data=[], error=str(exc))

    def _query_postgres(self, spec: QuerySpec) -> QueryResult:
        if not self._postgres_dsn:
            return QueryResult(source="postgres", data=[], error="not configured")
        try:
            conn = psycopg2.connect(self._postgres_dsn)
            conn.autocommit = True
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("SELECT layer, data_type, SUM(record_count) AS records FROM data_catalog GROUP BY layer, data_type LIMIT %s", (spec.limit,))
                rows = [dict(r) for r in cur.fetchall()]
            conn.close()
            return QueryResult(source="postgres", data=rows)
        except Exception as exc:
            return QueryResult(source="postgres", data=[], error=str(exc))

    def _query_neo4j(self, spec: QuerySpec) -> QueryResult:
        if self._neo4j is None:
            return QueryResult(source="neo4j", data=[], error="not configured")
        try:
            symbol = spec.filters.get("symbol")
            if symbol:
                rows = self._neo4j.run(
                    "MATCH (c:Company {symbol: $s}) RETURN c.symbol AS symbol, c.name AS name, c.sector AS sector",
                    {"s": symbol},
                )
            else:
                rows = self._neo4j.run(
                    "MATCH (c:Company) RETURN c.symbol AS symbol, c.name AS name, c.sector AS sector LIMIT $limit",
                    {"limit": spec.limit},
                )
            return QueryResult(source="neo4j", data=rows)
        except Exception as exc:
            return QueryResult(source="neo4j", data=[], error=str(exc))

    def _query_redis(self, spec: QuerySpec) -> QueryResult:
        if self._redis is None:
            return QueryResult(source="redis", data=[], error="not configured")
        try:
            symbol = spec.filters.get("symbol")
            if symbol:
                key = f"price:{symbol}"
                value = self._redis.get(key)
                data = [{"symbol": symbol, "price": value}] if value is not None else []
            else:
                data = []
            return QueryResult(source="redis", data=data)
        except Exception as exc:
            return QueryResult(source="redis", data=[], error=str(exc))
