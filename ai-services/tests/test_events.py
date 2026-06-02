"""Tests for ClaudeEventExtractor.

All Anthropic API calls and spaCy loads are mocked.
"""
from __future__ import annotations

import json
import time
from unittest.mock import MagicMock, patch

import pytest

from src.events.claude_extractor import ClaudeEventExtractor, _EVENT_TYPES


class TestClaudeEventExtractor:
    def _extractor_with_mock_client(self, response_text: str):
        extractor = ClaudeEventExtractor()
        mock_client = MagicMock()
        mock_response = MagicMock()
        mock_response.content = [MagicMock(text=response_text)]
        mock_client.messages.create.return_value = mock_response
        extractor._client = mock_client
        return extractor, mock_client

    def _valid_json_response(self, **overrides) -> str:
        base = {
            "event_type": "earnings",
            "companies": ["AAPL"],
            "date": "2024-01-15",
            "confidence": 0.92,
            "summary": "Apple reports record Q1 earnings.",
        }
        base.update(overrides)
        return json.dumps(base)

    # ─── Happy path ──────────────────────────────────────────────

    def test_extract_returns_all_required_keys(self):
        extractor, _ = self._extractor_with_mock_client(self._valid_json_response())
        result = extractor.extract("Apple Q1 earnings beat", source_id="art-001")
        for key in ("event_type", "companies", "date", "confidence", "summary", "source_id", "extracted_ms"):
            assert key in result

    def test_extract_source_id_attached(self):
        extractor, _ = self._extractor_with_mock_client(self._valid_json_response())
        result = extractor.extract("text", source_id="xyz-123")
        assert result["source_id"] == "xyz-123"

    def test_extract_earnings_event(self):
        extractor, _ = self._extractor_with_mock_client(self._valid_json_response())
        result = extractor.extract("Apple beats earnings", source_id="a1")
        assert result["event_type"] == "earnings"
        assert "AAPL" in result["companies"]
        assert result["confidence"] == 0.92

    def test_extracted_ms_is_recent(self):
        extractor, _ = self._extractor_with_mock_client(self._valid_json_response())
        before = int(time.time() * 1000)
        result = extractor.extract("text")
        after = int(time.time() * 1000)
        assert before <= result["extracted_ms"] <= after

    def test_strips_markdown_code_fences(self):
        raw = "```json\n" + self._valid_json_response() + "\n```"
        extractor, _ = self._extractor_with_mock_client(raw)
        result = extractor.extract("text")
        assert result["event_type"] == "earnings"

    def test_strips_code_fence_without_language(self):
        raw = "```\n" + self._valid_json_response() + "\n```"
        extractor, _ = self._extractor_with_mock_client(raw)
        result = extractor.extract("text")
        assert "event_type" in result

    def test_merger_event_type(self):
        extractor, _ = self._extractor_with_mock_client(
            self._valid_json_response(event_type="merger_acquisition", companies=["MSFT", "GOOGL"])
        )
        result = extractor.extract("Microsoft to acquire Google")
        assert result["event_type"] == "merger_acquisition"
        assert "MSFT" in result["companies"]

    # ─── Fallback ────────────────────────────────────────────────

    def test_spacy_fallback_on_api_error(self):
        extractor = ClaudeEventExtractor()
        mock_client = MagicMock()
        mock_client.messages.create.side_effect = Exception("rate_limit_exceeded")
        extractor._client = mock_client

        # Mock spaCy
        mock_nlp = MagicMock()
        mock_doc = MagicMock()
        mock_ent = MagicMock()
        mock_ent.text = "Tesla"
        mock_ent.label_ = "ORG"
        mock_doc.ents = [mock_ent]
        mock_nlp.return_value = mock_doc
        extractor._spacy_nlp = mock_nlp

        result = extractor.extract("Tesla announces new model", source_id="fb-1")
        assert result["event_type"] == "other"
        assert "Tesla" in result["companies"]
        assert result["confidence"] == 0.1
        assert "error" in result

    def test_fallback_without_spacy(self):
        extractor = ClaudeEventExtractor()
        mock_client = MagicMock()
        mock_client.messages.create.side_effect = Exception("api_error")
        extractor._client = mock_client
        extractor._spacy_nlp = False  # spaCy not available

        result = extractor.extract("Some financial news", source_id="nospacy")
        assert result["event_type"] == "other"
        assert result["companies"] == []
        assert "error" in result

    def test_null_source_id(self):
        extractor, _ = self._extractor_with_mock_client(self._valid_json_response())
        result = extractor.extract("text")
        assert result["source_id"] is None

    # ─── Event types validation ──────────────────────────────────

    def test_event_types_list_is_non_empty(self):
        assert len(_EVENT_TYPES) > 0
        assert "earnings" in _EVENT_TYPES
        assert "merger_acquisition" in _EVENT_TYPES
        assert "other" in _EVENT_TYPES

    # ─── Lazy loading ─────────────────────────────────────────────

    def test_client_lazy_loaded(self):
        extractor = ClaudeEventExtractor()
        assert extractor._client is None
