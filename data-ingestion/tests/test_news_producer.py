"""Tests for NewsProducer — deduplication, cashtag extraction, mock mode."""
import time
import uuid
from unittest.mock import MagicMock, patch

import pytest

from tests.conftest import avro_decode


@pytest.fixture
def news_producer(mock_kafka_producer):
    with patch.dict("os.environ", {"USE_MOCK_DATA": "true"}, clear=False):
        from src.producers.news_producer import NewsProducer
        p = NewsProducer()
        p._use_mock = True
        return p


class TestNewsProducerMockMode:
    def test_fetch_data_returns_articles(self, news_producer):
        articles = news_producer.fetch_data()
        assert isinstance(articles, list)
        assert len(articles) > 0

    def test_articles_marked_as_mock(self, news_producer):
        articles = news_producer.fetch_data()
        assert all(a["is_mock"] for a in articles)

    def test_transform_produces_avro_bytes(self, news_producer):
        articles = news_producer.fetch_data()
        results = news_producer.transform(articles)
        assert len(results) > 0
        for key, value in results:
            assert isinstance(value, bytes)

    def test_transform_avro_round_trip(self, news_producer):
        articles = news_producer.fetch_data()
        results = news_producer.transform(articles)
        for key, value in results:
            decoded = avro_decode("news_article", value)
            assert decoded["headline"]
            assert decoded["published_ms"] > 0


class TestNewsProducerDeduplication:
    def test_same_url_not_produced_twice(self, news_producer):
        article = {
            "article_id": str(uuid.uuid4()),
            "headline": "Test Headline",
            "body": "Body text",
            "url": "https://example.com/unique-article",
            "source": "TestSource",
            "published_ms": int(time.time() * 1000) - 1000,
            "ingested_ms": int(time.time() * 1000),
            "symbols": ["AAPL"],
            "language": "en",
            "is_mock": True,
        }
        r1 = news_producer.transform([article])
        assert len(r1) == 1

        # Same article again — should be deduplicated
        r2 = news_producer.transform([article])
        assert len(r2) == 0

    def test_different_urls_both_produced(self, news_producer):
        def make_article(url: str) -> dict:
            return {
                "article_id": str(uuid.uuid4()),
                "headline": "Headline",
                "body": None,
                "url": url,
                "source": "S",
                "published_ms": int(time.time() * 1000) - 1000,
                "ingested_ms": int(time.time() * 1000),
                "symbols": [],
                "language": "en",
                "is_mock": True,
            }

        r = news_producer.transform([
            make_article("https://example.com/a1"),
            make_article("https://example.com/a2"),
        ])
        assert len(r) == 2


class TestCashtagExtraction:
    def test_cashtag_extracted_from_headline(self, news_producer):
        text = "Check out $AAPL and $MSFT today"
        symbols = news_producer._extract_symbols(text)
        assert "AAPL" in symbols
        assert "MSFT" in symbols

    def test_unknown_cashtag_not_extracted(self, news_producer):
        text = "$XYZZY is not a watched symbol"
        symbols = news_producer._extract_symbols(text)
        assert "XYZZY" not in symbols

    def test_duplicate_cashtags_deduplicated(self, news_producer):
        text = "$AAPL is up, $AAPL breaks record"
        symbols = news_producer._extract_symbols(text)
        assert symbols.count("AAPL") == 1
