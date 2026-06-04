"""Tests for QualityChecker."""
from __future__ import annotations

import pytest
from src.quality.quality_checker import QualityChecker, QualityResult


@pytest.fixture
def checker():
    return QualityChecker()


class TestQualityChecker:
    def test_market_tick_valid(self, checker):
        r = checker.check("market_tick", {"symbol": "AAPL", "price": 150.0, "timestamp": 1700000000})
        assert r.passed
        assert len(r.violations) == 0

    def test_market_tick_missing_symbol(self, checker):
        r = checker.check("market_tick", {"price": 150.0, "timestamp": 1700000000})
        assert not r.passed
        assert any("symbol" in v for v in r.violations)

    def test_market_tick_negative_price(self, checker):
        r = checker.check("market_tick", {"symbol": "AAPL", "price": -1.0, "timestamp": 1700000000})
        assert not r.passed
        assert any("price" in v for v in r.violations)

    def test_market_tick_negative_volume(self, checker):
        r = checker.check("market_tick", {"symbol": "AAPL", "price": 150.0, "timestamp": 1700000000, "volume": -5})
        assert not r.passed

    def test_market_tick_nonnumeric_price(self, checker):
        r = checker.check("market_tick", {"symbol": "AAPL", "price": "not_a_number", "timestamp": 1700000000})
        assert not r.passed

    def test_news_article_valid(self, checker):
        r = checker.check("news_article", {"article_id": "a1", "title": "Test", "timestamp": 1700000000})
        assert r.passed

    def test_news_article_bad_sentiment(self, checker):
        r = checker.check("news_article", {
            "article_id": "a1", "title": "Test", "timestamp": 1700000000,
            "sentiment_score": 2.0
        })
        assert not r.passed

    def test_social_post_valid(self, checker):
        r = checker.check("social_post", {"post_id": "p1", "content": "AAPL!!", "timestamp": 1700000000})
        assert r.passed

    def test_social_post_missing_content(self, checker):
        r = checker.check("social_post", {"post_id": "p1", "timestamp": 1700000000})
        assert not r.passed

    def test_event_valid(self, checker):
        r = checker.check("event", {"event_id": "e1", "event_type": "earnings", "timestamp": 1700000000})
        assert r.passed

    def test_event_missing_event_type(self, checker):
        r = checker.check("event", {"event_id": "e1", "timestamp": 1700000000})
        assert not r.passed

    def test_unknown_type_passes(self, checker):
        r = checker.check("unknown_type", {"foo": "bar"})
        assert r.passed

    def test_score_all_pass(self, checker):
        results = [QualityResult(passed=True), QualityResult(passed=True)]
        assert checker.score(results) == 1.0

    def test_score_half_pass(self, checker):
        results = [QualityResult(passed=True), QualityResult(passed=False)]
        assert checker.score(results) == 0.5

    def test_score_empty(self, checker):
        assert checker.score([]) == 1.0

    def test_quality_result_bool(self):
        assert bool(QualityResult(passed=True))
        assert not bool(QualityResult(passed=False))
