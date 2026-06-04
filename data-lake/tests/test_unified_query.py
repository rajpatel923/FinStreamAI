"""Tests for UnifiedQuery."""
from __future__ import annotations

import pytest
from unittest.mock import MagicMock
from src.query.unified_query import UnifiedQuery, QuerySpec, QueryResult


class TestUnifiedQuery:
    def test_execute_neo4j_source(self, unified_query, neo4j_client):
        neo4j_client.run = MagicMock(return_value=[{"symbol": "AAPL", "name": "Apple", "sector": "Tech"}])
        spec = QuerySpec(sources=["neo4j"], filters={"symbol": "AAPL"})
        results = unified_query.execute(spec)
        assert "neo4j" in results
        assert results["neo4j"].data[0]["symbol"] == "AAPL"
        assert results["neo4j"].error is None

    def test_execute_redis_source(self, unified_query, redis_cache):
        redis_cache.set("price:AAPL", 150.0, 60)
        spec = QuerySpec(sources=["redis"], filters={"symbol": "AAPL"})
        results = unified_query.execute(spec)
        assert "redis" in results
        assert results["redis"].data[0]["price"] == 150.0

    def test_execute_redis_no_symbol(self, unified_query):
        spec = QuerySpec(sources=["redis"], filters={})
        results = unified_query.execute(spec)
        assert "redis" in results
        assert results["redis"].data == []

    def test_execute_timescale_not_configured(self, unified_query):
        spec = QuerySpec(sources=["timescale"], filters={})
        results = unified_query.execute(spec)
        assert "timescale" in results
        assert results["timescale"].error == "not configured"

    def test_execute_postgres_not_configured(self, unified_query):
        spec = QuerySpec(sources=["postgres"], filters={})
        results = unified_query.execute(spec)
        assert "postgres" in results
        assert results["postgres"].error == "not configured"

    def test_execute_neo4j_not_configured(self):
        uq = UnifiedQuery(neo4j_client=None)
        spec = QuerySpec(sources=["neo4j"], filters={})
        results = uq.execute(spec)
        assert results["neo4j"].error == "not configured"

    def test_execute_redis_not_configured(self):
        uq = UnifiedQuery(redis_cache=None)
        spec = QuerySpec(sources=["redis"], filters={"symbol": "AAPL"})
        results = uq.execute(spec)
        assert results["redis"].error == "not configured"

    def test_execute_multiple_sources(self, unified_query, neo4j_client, redis_cache):
        neo4j_client.run = MagicMock(return_value=[])
        redis_cache.set("price:MSFT", 300.0, 60)
        spec = QuerySpec(sources=["neo4j", "redis"], filters={"symbol": "MSFT"})
        results = unified_query.execute(spec)
        assert "neo4j" in results
        assert "redis" in results

    def test_execute_unknown_source_ignored(self, unified_query):
        spec = QuerySpec(sources=["unknown_db"], filters={})
        results = unified_query.execute(spec)
        assert "unknown_db" not in results

    def test_query_result_dataclass(self):
        r = QueryResult(source="test", data=[{"x": 1}])
        assert r.source == "test"
        assert r.error is None

    def test_neo4j_error_captured(self, unified_query, neo4j_client):
        neo4j_client.run = MagicMock(side_effect=RuntimeError("Neo4j down"))
        spec = QuerySpec(sources=["neo4j"], filters={"symbol": "AAPL"})
        results = unified_query.execute(spec)
        assert results["neo4j"].error is not None
