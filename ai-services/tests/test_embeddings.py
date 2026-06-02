"""Tests for ChromaService.

sentence-transformers and ChromaDB are mocked — no model download or disk I/O.
"""
from __future__ import annotations

from unittest.mock import MagicMock, call

import numpy as np
import pytest

from src.embeddings.chroma_service import ChromaService


class TestChromaService:
    def _service_with_mocks(self):
        svc = ChromaService(persist_dir="/tmp/test_chroma")
        # Mock sentence transformer
        mock_model = MagicMock()
        mock_model.encode.return_value = np.array([[0.1] * 384])
        svc._model = mock_model
        # Mock ChromaDB collection
        mock_collection = MagicMock()
        mock_collection.count.return_value = 0
        svc._collection = mock_collection
        return svc, mock_model, mock_collection

    # ─── add_document ────────────────────────────────────────────

    def test_add_document_calls_upsert(self):
        svc, mock_model, mock_collection = self._service_with_mocks()
        svc.add_document("doc-1", "Apple earnings beat", {"symbol": "AAPL"})
        mock_collection.upsert.assert_called_once()
        args = mock_collection.upsert.call_args
        assert args.kwargs["ids"] == ["doc-1"]
        assert args.kwargs["documents"] == ["Apple earnings beat"]
        assert args.kwargs["metadatas"] == [{"symbol": "AAPL"}]

    def test_add_document_encodes_text(self):
        svc, mock_model, mock_collection = self._service_with_mocks()
        svc.add_document("d1", "Fed rate decision")
        mock_model.encode.assert_called_once_with(["Fed rate decision"])

    def test_add_document_no_metadata_uses_empty_dict(self):
        svc, mock_model, mock_collection = self._service_with_mocks()
        svc.add_document("d1", "text")
        kwargs = mock_collection.upsert.call_args.kwargs
        assert kwargs["metadatas"] == [{}]

    # ─── search_similar ──────────────────────────────────────────

    def test_search_empty_collection_returns_empty(self):
        svc, _, mock_collection = self._service_with_mocks()
        mock_collection.count.return_value = 0
        result = svc.search_similar("query")
        assert result == []

    def test_search_returns_results(self):
        svc, mock_model, mock_collection = self._service_with_mocks()
        mock_collection.count.return_value = 3
        mock_collection.query.return_value = {
            "ids": [["doc-1", "doc-2"]],
            "documents": [["Apple news", "Fed news"]],
            "metadatas": [[{"sym": "AAPL"}, {"sym": ""}]],
            "distances": [[0.1, 0.3]],
        }
        results = svc.search_similar("Apple earnings", n_results=2)
        assert len(results) == 2
        assert results[0]["id"] == "doc-1"
        assert results[0]["text"] == "Apple news"
        assert results[0]["distance"] == 0.1

    def test_search_limits_n_results_to_collection_count(self):
        svc, mock_model, mock_collection = self._service_with_mocks()
        mock_collection.count.return_value = 2
        mock_collection.query.return_value = {
            "ids": [["d1"]],
            "documents": [["text"]],
            "metadatas": [[{}]],
            "distances": [[0.05]],
        }
        svc.search_similar("q", n_results=10)
        query_call = mock_collection.query.call_args
        # n_results should be min(10, 2) = 2
        assert query_call.kwargs["n_results"] == 2

    def test_search_distance_rounded(self):
        svc, mock_model, mock_collection = self._service_with_mocks()
        mock_collection.count.return_value = 1
        mock_collection.query.return_value = {
            "ids": [["d1"]],
            "documents": [["t"]],
            "metadatas": [[{}]],
            "distances": [[0.123456789]],
        }
        results = svc.search_similar("q")
        assert results[0]["distance"] == round(0.123456789, 4)

    # ─── count ───────────────────────────────────────────────────

    def test_count_delegates_to_collection(self):
        svc, _, mock_collection = self._service_with_mocks()
        mock_collection.count.return_value = 42
        assert svc.count() == 42

    # ─── lazy loading ─────────────────────────────────────────────

    def test_model_lazy_loaded(self):
        svc = ChromaService()
        assert svc._model is None

    def test_collection_lazy_loaded(self):
        svc = ChromaService()
        assert svc._collection is None
