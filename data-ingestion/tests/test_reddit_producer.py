"""Tests for RedditProducer — cashtag extraction, mock mode, deduplication."""
import time
import uuid
from unittest.mock import MagicMock, patch

import pytest

from tests.conftest import avro_decode


@pytest.fixture
def reddit_producer(mock_kafka_producer):
    with patch.dict("os.environ", {"USE_MOCK_DATA": "true"}, clear=False):
        from src.producers.social_media_producer import RedditProducer
        p = RedditProducer()
        p._use_mock = True
        return p


class TestRedditProducerMockMode:
    def test_fetch_data_returns_posts(self, reddit_producer):
        posts = reddit_producer.fetch_data()
        assert isinstance(posts, list)
        assert len(posts) > 0

    def test_posts_marked_as_mock(self, reddit_producer):
        posts = reddit_producer.fetch_data()
        assert all(p["is_mock"] for p in posts)

    def test_posts_have_required_fields(self, reddit_producer):
        posts = reddit_producer.fetch_data()
        required = {"post_id", "platform", "title", "created_ms"}
        for post in posts:
            assert required <= post.keys()

    def test_transform_produces_avro_bytes(self, reddit_producer):
        posts = reddit_producer.fetch_data()
        results = reddit_producer.transform(posts)
        assert len(results) > 0
        for key, value in results:
            assert isinstance(value, bytes)

    def test_transform_avro_round_trip(self, reddit_producer):
        posts = reddit_producer.fetch_data()
        results = reddit_producer.transform(posts)
        for key, value in results:
            decoded = avro_decode("social_post", value)
            assert decoded["platform"] == "reddit"
            assert decoded["created_ms"] > 0


class TestRedditDeduplication:
    def test_same_post_id_not_produced_twice(self, reddit_producer):
        post = {
            "post_id": "uniqueid123",
            "platform": "reddit",
            "subreddit": "stocks",
            "title": "$AAPL is great",
            "body": None,
            "author": "user1",
            "score": 100,
            "num_comments": 20,
            "created_ms": int(time.time() * 1000),
            "symbols": ["AAPL"],
            "url": "https://reddit.com/r/stocks/comments/uniqueid123",
            "is_mock": True,
        }
        r1 = reddit_producer.transform([post])
        assert len(r1) == 1

        r2 = reddit_producer.transform([post])
        assert len(r2) == 0

    def test_different_post_ids_both_produced(self, reddit_producer):
        def make_post(pid: str) -> dict:
            return {
                "post_id": pid,
                "platform": "reddit",
                "subreddit": "stocks",
                "title": "Some title",
                "body": None,
                "author": "user",
                "score": 0,
                "num_comments": 0,
                "created_ms": int(time.time() * 1000),
                "symbols": [],
                "url": f"https://reddit.com/r/stocks/{pid}",
                "is_mock": True,
            }

        results = reddit_producer.transform([make_post("pid1"), make_post("pid2")])
        assert len(results) == 2


class TestCashtagRegex:
    def test_cashtag_extracted(self, reddit_producer):
        symbols = reddit_producer._extract_symbols("$AAPL to the moon!")
        assert "AAPL" in symbols

    def test_lowercase_not_extracted(self, reddit_producer):
        symbols = reddit_producer._extract_symbols("$aapl won't match")
        assert "aapl" not in symbols
        assert "AAPL" not in symbols

    def test_unwatched_symbol_excluded(self, reddit_producer):
        symbols = reddit_producer._extract_symbols("$ZZZZ is not tracked")
        assert "ZZZZ" not in symbols

    def test_multiple_symbols_extracted(self, reddit_producer):
        symbols = reddit_producer._extract_symbols("Buying $AAPL and $NVDA puts")
        assert "AAPL" in symbols
        assert "NVDA" in symbols

    def test_dedup_in_extraction(self, reddit_producer):
        symbols = reddit_producer._extract_symbols("$TSLA $TSLA $TSLA")
        assert symbols.count("TSLA") == 1
