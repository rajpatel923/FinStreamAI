"""Integration-style tests for all FastAPI route handlers.

A lightweight test app is assembled with mocked service state so that
no real ML models, databases, or Kafka brokers are needed.
"""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.api import embeddings, events, health, prediction, risk, sentiment


@pytest.fixture
def client():
    """Minimal FastAPI app with all routers and mocked app.state."""
    app = FastAPI()

    # ── Mock services ────────────────────────────────────────────
    mock_finbert = MagicMock()
    mock_finbert.analyze.return_value = [
        {"label": "positive", "score": 0.97, "sentiment_score": 1.0}
    ]
    mock_finbert.analyze_one.return_value = {
        "label": "positive", "score": 0.97, "sentiment_score": 1.0
    }

    mock_ner = MagicMock()
    mock_ner.extract_entities.return_value = {
        "companies": ["Apple"], "persons": ["Tim Cook"], "locations": []
    }

    mock_extractor = MagicMock()
    mock_extractor.extract.return_value = {
        "event_type": "earnings",
        "companies": ["AAPL"],
        "date": "2024-01-15",
        "confidence": 0.9,
        "summary": "Apple Q1 earnings beat.",
        "source_id": None,
        "extracted_ms": 1700000000000,
    }

    mock_chroma = MagicMock()
    mock_chroma.search_similar.return_value = [
        {"id": "doc-1", "text": "Fed cuts rates", "metadata": {}, "distance": 0.12}
    ]
    mock_chroma.count.return_value = 1

    mock_predictor = MagicMock()
    mock_predictor.predict.return_value = {
        "symbol": "AAPL", "direction": "up", "confidence": 0.72,
        "up_probability": 0.72, "horizon": "1hr"
    }
    mock_predictor.predict_batch.return_value = [
        {"symbol": "AAPL", "direction": "up", "confidence": 0.72,
         "up_probability": 0.72, "horizon": "1hr"},
    ]
    mock_predictor.get_shap_values.return_value = {
        "symbol": "AAPL", "shap_values": {"return_1": 0.1}
    }

    mock_risk = MagicMock()
    mock_risk.compute_metrics.return_value = {
        "symbol": "AAPL", "var_95": -0.023, "sharpe_ratio": 1.4
    }
    mock_risk.compute_portfolio_risk.return_value = {
        "symbol": "PORTFOLIO", "var_95": -0.018
    }
    mock_risk.backtest.return_value = {
        "symbol": "AAPL", "strategy": "buy_hold", "total_return": 0.12
    }

    app.state.finbert = mock_finbert
    app.state.ner = mock_ner
    app.state.extractor = mock_extractor
    app.state.chroma = mock_chroma
    app.state.predictor = mock_predictor
    app.state.risk = mock_risk

    prefix = "/api/v1"
    app.include_router(health.router, prefix=prefix)
    app.include_router(sentiment.router, prefix=prefix)
    app.include_router(events.router, prefix=prefix)
    app.include_router(embeddings.router, prefix=prefix)
    app.include_router(prediction.router, prefix=prefix)
    app.include_router(risk.router, prefix=prefix)

    return TestClient(app)


# ─── Health ──────────────────────────────────────────────────────────────────

class TestHealthRoutes:
    def test_health_200(self, client):
        r = client.get("/api/v1/health")
        assert r.status_code == 200
        assert r.json()["status"] == "ok"

    def test_health_has_uptime(self, client):
        r = client.get("/api/v1/health")
        assert "uptime_s" in r.json()

    def test_ready_200(self, client):
        r = client.get("/api/v1/ready")
        assert r.status_code == 200

    def test_live_200(self, client):
        r = client.get("/api/v1/live")
        assert r.status_code == 200
        assert r.json()["status"] == "alive"


# ─── Sentiment ───────────────────────────────────────────────────────────────

class TestSentimentRoutes:
    def test_analyze_single_text(self, client):
        r = client.post("/api/v1/sentiment/analyze", json={"text": "Apple beats earnings"})
        assert r.status_code == 200
        body = r.json()
        assert body["label"] == "positive"
        assert "score" in body

    def test_analyze_batch_texts(self, client):
        r = client.post(
            "/api/v1/sentiment/analyze",
            json={"texts": ["good news", "bad news"]},
        )
        assert r.status_code == 200
        body = r.json()
        assert "results" in body
        assert body["count"] == 1  # mock returns 1 item regardless

    def test_analyze_no_input(self, client):
        r = client.post("/api/v1/sentiment/analyze", json={})
        assert r.status_code == 200
        assert "error" in r.json()

    def test_entities_endpoint(self, client):
        r = client.post(
            "/api/v1/sentiment/entities",
            json={"text": "Tim Cook presented Apple's results"},
        )
        assert r.status_code == 200
        body = r.json()
        assert "companies" in body
        assert "Apple" in body["companies"]

    def test_entities_no_text(self, client):
        r = client.post("/api/v1/sentiment/entities", json={})
        assert r.status_code == 200
        assert "error" in r.json()


# ─── Events ──────────────────────────────────────────────────────────────────

class TestEventsRoutes:
    def test_extract_event(self, client):
        r = client.post(
            "/api/v1/events/extract",
            json={"text": "Apple Q1 earnings beat expectations", "source_id": "art-1"},
        )
        assert r.status_code == 200
        body = r.json()
        assert body["event_type"] == "earnings"
        assert "AAPL" in body["companies"]

    def test_extract_without_source_id(self, client):
        r = client.post("/api/v1/events/extract", json={"text": "Some news"})
        assert r.status_code == 200


# ─── Embeddings ──────────────────────────────────────────────────────────────

class TestEmbeddingsRoutes:
    def test_search_similar(self, client):
        r = client.get("/api/v1/search/similar?q=federal+reserve+rate")
        assert r.status_code == 200
        body = r.json()
        assert "results" in body
        assert body["query"] == "federal reserve rate"

    def test_search_default_n(self, client):
        r = client.get("/api/v1/search/similar?q=earnings")
        assert r.status_code == 200

    def test_add_document(self, client):
        r = client.post(
            "/api/v1/search/documents",
            json={"doc_id": "art-1", "text": "Apple earnings", "metadata": {"sym": "AAPL"}},
        )
        assert r.status_code == 200
        assert r.json()["doc_id"] == "art-1"


# ─── Prediction ──────────────────────────────────────────────────────────────

class TestPredictionRoutes:
    def test_get_signal(self, client):
        r = client.get("/api/v1/predict/signals/AAPL")
        assert r.status_code == 200
        body = r.json()
        assert body["symbol"] == "AAPL"
        assert body["direction"] in ("up", "down", "neutral")
        assert body["horizon"] == "1hr"

    def test_batch_predict(self, client):
        r = client.post(
            "/api/v1/predict/batch",
            json={"symbols": ["AAPL", "MSFT"]},
        )
        assert r.status_code == 200
        body = r.json()
        assert "results" in body

    def test_shap_values(self, client):
        r = client.get("/api/v1/predict/shap/AAPL")
        assert r.status_code == 200
        assert "shap_values" in r.json()

    def test_symbol_uppercased(self, client):
        r = client.get("/api/v1/predict/signals/aapl")
        assert r.status_code == 200
        client.app.state.predictor.predict.assert_called_with("AAPL")


# ─── Risk ────────────────────────────────────────────────────────────────────

class TestRiskRoutes:
    def test_risk_metrics(self, client):
        r = client.get("/api/v1/risk/metrics/AAPL")
        assert r.status_code == 200
        body = r.json()
        assert body["symbol"] == "AAPL"
        assert "var_95" in body

    def test_portfolio_risk(self, client):
        r = client.post(
            "/api/v1/risk/portfolio",
            json={"positions": [{"symbol": "AAPL", "weight": 0.5}, {"symbol": "MSFT", "weight": 0.5}]},
        )
        assert r.status_code == 200

    def test_backtest(self, client):
        r = client.post(
            "/api/v1/risk/backtest",
            json={"symbol": "AAPL", "strategy": "buy_hold"},
        )
        assert r.status_code == 200
        body = r.json()
        assert body["strategy"] == "buy_hold"
