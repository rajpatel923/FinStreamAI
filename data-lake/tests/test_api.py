"""Tests for Data Lake API endpoints."""
from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pandas as pd
import pytest
from fastapi.testclient import TestClient

# Ensure imports work
_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


def _make_app(state_overrides: dict | None = None):
    """Build a minimal FastAPI app with mocked state for testing."""
    from fastapi import FastAPI
    from fastapi.middleware.cors import CORSMiddleware
    from src.api import health, lake_router, graph_router, query_router, cache_router

    app = FastAPI()
    app.include_router(health.router, prefix="/api/v1")
    app.include_router(lake_router.router, prefix="/api/v1")
    app.include_router(graph_router.router, prefix="/api/v1")
    app.include_router(query_router.router, prefix="/api/v1")
    app.include_router(cache_router.router, prefix="/api/v1")
    app.add_api_route("/health", health.health, include_in_schema=False)
    app.add_api_route("/ready", health.ready, include_in_schema=False)
    app.add_api_route("/live", health.live, include_in_schema=False)

    for key, val in (state_overrides or {}).items():
        setattr(app.state, key, val)

    return app


# ------------------------------------------------------------------
# Health
# ------------------------------------------------------------------
class TestHealthEndpoints:
    def test_health(self):
        app = _make_app()
        with TestClient(app) as client:
            r = client.get("/health")
        assert r.status_code == 200
        assert r.json()["status"] == "healthy"

    def test_live(self):
        app = _make_app()
        with TestClient(app) as client:
            r = client.get("/live")
        assert r.status_code == 200

    def test_ready_no_deps(self):
        app = _make_app()
        with TestClient(app) as client:
            r = client.get("/ready")
        assert r.status_code == 200

    def test_ready_redis_ok(self):
        mock_cache = MagicMock()
        mock_cache.client.ping.return_value = True
        app = _make_app({"cache": mock_cache})
        with TestClient(app) as client:
            r = client.get("/ready")
        assert r.status_code == 200

    def test_ready_redis_error(self):
        mock_cache = MagicMock()
        mock_cache.client.ping.side_effect = Exception("Redis down")
        app = _make_app({"cache": mock_cache})
        with TestClient(app) as client:
            r = client.get("/ready")
        assert r.status_code == 503


# ------------------------------------------------------------------
# Lake Router
# ------------------------------------------------------------------
class TestLakeRouter:
    def test_quality_no_quarantine(self):
        app = _make_app()
        with TestClient(app) as client:
            r = client.get("/api/v1/lake/quality")
        assert r.status_code == 503

    def test_quality_returns_report(self):
        mock_q = MagicMock()
        mock_q.quality_report.return_value = {"market_tick": {"total": 10, "quarantined": 1, "pass_rate": 0.9}}
        app = _make_app({"quarantine": mock_q})
        with TestClient(app) as client:
            r = client.get("/api/v1/lake/quality")
        assert r.status_code == 200
        assert "market_tick" in r.json()

    def test_partition_stats(self):
        mock_cat = MagicMock()
        mock_cat.get_stats.return_value = [{"layer": "bronze", "data_type": "market_tick", "total_records": 1000}]
        app = _make_app({"catalog": mock_cat})
        with TestClient(app) as client:
            r = client.get("/api/v1/lake/partitions")
        assert r.status_code == 200

    def test_trigger_silver(self):
        mock_silver = MagicMock()
        mock_silver.process_market_data.return_value = 100
        mock_silver.process_news.return_value = 50
        mock_silver.process_social.return_value = 25
        app = _make_app({"silver_layer": mock_silver})
        with TestClient(app) as client:
            r = client.post("/api/v1/lake/ingest/silver")
        assert r.status_code == 200
        assert r.json()["status"] == "ok"

    def test_trigger_gold(self):
        mock_gold = MagicMock()
        mock_gold.build_features.return_value = pd.DataFrame({"a": [1, 2]})
        mock_gold.build_signals.return_value = pd.DataFrame({"b": [3, 4]})
        mock_gold.build_analytics.return_value = pd.DataFrame()
        app = _make_app({"gold_layer": mock_gold})
        with TestClient(app) as client:
            r = client.post("/api/v1/lake/ingest/gold")
        assert r.status_code == 200


# ------------------------------------------------------------------
# Graph Router
# ------------------------------------------------------------------
class TestGraphRouter:
    def _kg_state(self):
        kg = MagicMock()
        kg.get_company.return_value = {"symbol": "AAPL", "name": "Apple", "sector": "Technology"}
        kg.get_company_network.return_value = []
        kg.import_company.return_value = {"symbol": "AAPL"}
        kg.list_companies.return_value = []
        kg.pagerank.return_value = []
        kg.find_affected_companies.return_value = []
        return {"knowledge_graph": kg}

    def test_get_company(self):
        app = _make_app(self._kg_state())
        with TestClient(app) as client:
            r = client.get("/api/v1/graph/company/AAPL")
        assert r.status_code == 200
        assert r.json()["company"]["symbol"] == "AAPL"

    def test_get_company_not_found(self):
        kg = MagicMock()
        kg.get_company.return_value = None
        app = _make_app({"knowledge_graph": kg})
        with TestClient(app) as client:
            r = client.get("/api/v1/graph/company/ZZZZ")
        assert r.status_code == 404

    def test_upsert_company(self):
        app = _make_app(self._kg_state())
        with TestClient(app) as client:
            r = client.post("/api/v1/graph/company", json={"symbol": "AAPL", "name": "Apple", "sector": "Technology"})
        assert r.status_code == 200

    def test_list_companies(self):
        app = _make_app(self._kg_state())
        with TestClient(app) as client:
            r = client.get("/api/v1/graph/companies")
        assert r.status_code == 200

    def test_pagerank(self):
        app = _make_app(self._kg_state())
        with TestClient(app) as client:
            r = client.get("/api/v1/graph/pagerank")
        assert r.status_code == 200

    def test_no_knowledge_graph(self):
        app = _make_app()
        with TestClient(app) as client:
            r = client.get("/api/v1/graph/company/AAPL")
        assert r.status_code == 503


# ------------------------------------------------------------------
# Query Router
# ------------------------------------------------------------------
class TestQueryRouter:
    def test_unified_query(self):
        from src.query.unified_query import QueryResult
        mock_uq = MagicMock()
        mock_uq.execute.return_value = {
            "redis": QueryResult(source="redis", data=[{"symbol": "AAPL", "price": 150.0}])
        }
        app = _make_app({"unified_query": mock_uq})
        with TestClient(app) as client:
            r = client.post("/api/v1/query", json={"sources": ["redis"], "filters": {"symbol": "AAPL"}})
        assert r.status_code == 200
        assert "redis" in r.json()

    def test_query_no_engine(self):
        app = _make_app()
        with TestClient(app) as client:
            r = client.post("/api/v1/query", json={"sources": ["redis"]})
        assert r.status_code == 503


# ------------------------------------------------------------------
# Cache Router
# ------------------------------------------------------------------
class TestCacheRouter:
    def test_stats(self):
        mock_cache = MagicMock()
        mock_cache.get_stats.return_value = {"hits": 10, "misses": 2}
        app = _make_app({"cache": mock_cache})
        with TestClient(app) as client:
            r = client.get("/api/v1/cache/stats")
        assert r.status_code == 200

    def test_invalidate_key(self):
        mock_cache = MagicMock()
        mock_cache.delete.return_value = True
        app = _make_app({"cache": mock_cache})
        with TestClient(app) as client:
            r = client.delete("/api/v1/cache/key/price:AAPL")
        assert r.status_code == 200
        assert r.json()["deleted"] is True

    def test_warm_no_cache(self):
        app = _make_app()
        with TestClient(app) as client:
            r = client.post("/api/v1/cache/warm", json={"symbols": ["AAPL"]})
        assert r.status_code == 503

    def test_warm_with_cache(self):
        mock_cache = MagicMock()
        mock_cache.warm_prices.return_value = 1
        app = _make_app({"cache": mock_cache})
        with TestClient(app) as client:
            r = client.post("/api/v1/cache/warm", json={"symbols": ["AAPL"]})
        assert r.status_code == 200
        assert r.json()["loaded"] == 1
