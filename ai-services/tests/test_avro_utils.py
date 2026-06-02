"""Tests for AIAvroSerializer — serialize/deserialize round trips."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from src.utils.avro_utils import AIAvroSerializer


@pytest.fixture
def serializer():
    return AIAvroSerializer("http://localhost:8081")


# ─── _load_schema ────────────────────────────────────────────────────────────

class TestLoadSchema:
    def test_loads_news_article_schema(self, serializer):
        schema = serializer._load_schema("news_article")
        assert schema["name"] == "NewsArticle"
        assert "fields" in schema

    def test_loads_social_post_schema(self, serializer):
        schema = serializer._load_schema("social_post")
        assert schema["name"] == "SocialPost"

    def test_loads_news_sentiment_schema(self, serializer):
        schema = serializer._load_schema("news_sentiment")
        assert schema["name"] == "NewsSentiment"

    def test_loads_social_sentiment_schema(self, serializer):
        schema = serializer._load_schema("social_sentiment")
        assert schema["name"] == "SocialSentiment"

    def test_schema_cached_after_first_load(self, serializer):
        s1 = serializer._load_schema("news_article")
        s2 = serializer._load_schema("news_article")
        assert s1 is s2  # same object — cache hit


# ─── register ────────────────────────────────────────────────────────────────

class TestRegister:
    def test_register_returns_schema_id(self, serializer):
        # patch_schema_registry fixture in conftest mocks the httpx.post
        schema_id = serializer.register("news_article-value", "news_article")
        assert schema_id == 1

    def test_register_cached(self, serializer):
        id1 = serializer.register("news_article-value", "news_article")
        id2 = serializer.register("news_article-value", "news_article")
        assert id1 == id2

    def test_register_fallback_on_error(self):
        s = AIAvroSerializer("http://bad-host:9999")
        with patch("src.utils.avro_utils.httpx.post", side_effect=Exception("timeout")):
            schema_id = s.register("test-value", "news_article")
        assert schema_id == 1


# ─── serialize / deserialize round-trip ─────────────────────────────────────

class TestSerializeDeserialize:
    def _news_sentiment_record(self):
        return {
            "id": "art-001",
            "symbols": ["AAPL", "MSFT"],
            "sentiment_score": 0.85,
            "confidence": 0.92,
            "scored_ms": 1700000000000,
            "source": "finbert",
        }

    def _social_sentiment_record(self):
        return {
            "id": "post-001",
            "symbols": ["TSLA"],
            "sentiment_score": -0.5,
            "confidence": 0.78,
            "scored_ms": 1700000001000,
            "source": "reddit",
        }

    def test_news_sentiment_round_trip(self, serializer):
        record = self._news_sentiment_record()
        encoded = serializer.serialize("news_sentiment", record)
        decoded = serializer.deserialize("news_sentiment", encoded)
        assert decoded["id"] == record["id"]
        assert decoded["sentiment_score"] == record["sentiment_score"]
        assert decoded["symbols"] == record["symbols"]

    def test_social_sentiment_round_trip(self, serializer):
        record = self._social_sentiment_record()
        encoded = serializer.serialize("social_sentiment", record)
        decoded = serializer.deserialize("social_sentiment", encoded)
        assert decoded["id"] == record["id"]
        assert decoded["confidence"] == record["confidence"]

    def test_serialized_bytes_start_with_magic_byte(self, serializer):
        record = self._news_sentiment_record()
        data = serializer.serialize("news_sentiment", record)
        assert data[0] == 0  # magic byte

    def test_deserialize_wrong_magic_raises(self, serializer):
        with pytest.raises(ValueError, match="magic byte"):
            # Corrupt the magic byte
            serializer.deserialize("news_sentiment", b"\x01\x00\x00\x00\x01" + b"\x00" * 20)

    def test_serialize_uses_custom_subject(self, serializer):
        record = self._news_sentiment_record()
        # Should not raise
        encoded = serializer.serialize("news_sentiment", record, subject="my-custom-subject")
        decoded = serializer.deserialize("news_sentiment", encoded)
        assert decoded["id"] == record["id"]
