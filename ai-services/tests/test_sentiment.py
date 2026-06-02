"""Tests for FinBERTService and NERService.

All HuggingFace pipeline calls are mocked — no model download required.
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from src.sentiment.finbert_service import FinBERTService, _LABEL_TO_SCORE
from src.sentiment.ner_service import NERService


# ─── FinBERTService ──────────────────────────────────────────────────────────

class TestFinBERTService:
    def _service_with_mock_pipeline(self, side_effect=None):
        """Return a FinBERTService whose pipeline is a MagicMock."""
        svc = FinBERTService()
        mock_pipe = MagicMock(side_effect=side_effect)
        svc._pipeline = mock_pipe
        return svc, mock_pipe

    def test_analyze_positive(self):
        svc, mock_pipe = self._service_with_mock_pipeline()
        mock_pipe.return_value = [{"label": "positive", "score": 0.97}]
        result = svc.analyze(["Apple beats earnings expectations"])
        assert len(result) == 1
        assert result[0]["label"] == "positive"
        assert result[0]["score"] == 0.97
        assert result[0]["sentiment_score"] == 1.0

    def test_analyze_negative(self):
        svc, mock_pipe = self._service_with_mock_pipeline()
        mock_pipe.return_value = [{"label": "negative", "score": 0.88}]
        result = svc.analyze(["Company files for bankruptcy"])
        assert result[0]["label"] == "negative"
        assert result[0]["sentiment_score"] == -1.0

    def test_analyze_neutral(self):
        svc, mock_pipe = self._service_with_mock_pipeline()
        mock_pipe.return_value = [{"label": "neutral", "score": 0.75}]
        result = svc.analyze(["Markets opened flat today"])
        assert result[0]["sentiment_score"] == 0.0

    def test_analyze_batch(self):
        svc, mock_pipe = self._service_with_mock_pipeline()
        mock_pipe.return_value = [
            {"label": "positive", "score": 0.9},
            {"label": "negative", "score": 0.8},
        ]
        results = svc.analyze(["good news", "bad news"])
        assert len(results) == 2
        assert results[0]["sentiment_score"] == 1.0
        assert results[1]["sentiment_score"] == -1.0

    def test_analyze_empty_returns_empty(self):
        svc = FinBERTService()
        result = svc.analyze([])
        assert result == []

    def test_analyze_one_delegates_to_analyze(self):
        svc, mock_pipe = self._service_with_mock_pipeline()
        mock_pipe.return_value = [{"label": "positive", "score": 0.95}]
        result = svc.analyze_one("Fed rate cut expected")
        assert isinstance(result, dict)
        assert "label" in result

    def test_label_to_score_mapping(self):
        assert _LABEL_TO_SCORE["positive"] == 1.0
        assert _LABEL_TO_SCORE["neutral"] == 0.0
        assert _LABEL_TO_SCORE["negative"] == -1.0

    def test_score_rounded_to_4dp(self):
        svc, mock_pipe = self._service_with_mock_pipeline()
        mock_pipe.return_value = [{"label": "positive", "score": 0.9876543}]
        result = svc.analyze(["test"])
        assert result[0]["score"] == round(0.9876543, 4)

    def test_pipeline_lazy_loaded(self):
        svc = FinBERTService()
        assert svc._pipeline is None  # not loaded until first call

    def test_pipeline_cached_after_first_call(self):
        svc, mock_pipe = self._service_with_mock_pipeline()
        mock_pipe.return_value = [{"label": "neutral", "score": 0.5}]
        svc.analyze(["test"])
        svc.analyze(["test again"])
        # _get_pipeline should not recreate the object
        assert svc._pipeline is mock_pipe

    def test_label_case_insensitive(self):
        svc, mock_pipe = self._service_with_mock_pipeline()
        mock_pipe.return_value = [{"label": "POSITIVE", "score": 0.9}]
        result = svc.analyze(["test"])
        assert result[0]["label"] == "positive"
        assert result[0]["sentiment_score"] == 1.0


# ─── NERService ───────────────────────────────────────────────────────────────

class TestNERService:
    def _service_with_mock_pipeline(self):
        svc = NERService()
        mock_pipe = MagicMock()
        svc._pipeline = mock_pipe
        return svc, mock_pipe

    def test_extracts_organisations(self):
        svc, mock_pipe = self._service_with_mock_pipeline()
        mock_pipe.return_value = [
            {"entity_group": "ORG", "word": "Apple"},
            {"entity_group": "ORG", "word": "Microsoft"},
            {"entity_group": "PER", "word": "Tim Cook"},
        ]
        result = svc.extract_entities("Tim Cook of Apple and Microsoft")
        assert "Apple" in result["companies"]
        assert "Microsoft" in result["companies"]
        assert "Tim Cook" in result["persons"]
        assert result["locations"] == []

    def test_deduplicates_entities(self):
        svc, mock_pipe = self._service_with_mock_pipeline()
        mock_pipe.return_value = [
            {"entity_group": "ORG", "word": "Tesla"},
            {"entity_group": "ORG", "word": "Tesla"},
        ]
        result = svc.extract_entities("Tesla Tesla Tesla")
        assert len(result["companies"]) == 1

    def test_empty_entities(self):
        svc, mock_pipe = self._service_with_mock_pipeline()
        mock_pipe.return_value = []
        result = svc.extract_entities("some generic text")
        assert result == {"companies": [], "persons": [], "locations": []}

    def test_extracts_locations(self):
        svc, mock_pipe = self._service_with_mock_pipeline()
        mock_pipe.return_value = [
            {"entity_group": "LOC", "word": "New York"},
        ]
        result = svc.extract_entities("Markets in New York")
        assert "New York" in result["locations"]

    def test_pipeline_lazy_loaded(self):
        svc = NERService()
        assert svc._pipeline is None
