"""Tests for Quarantine."""
from __future__ import annotations

import pytest
from src.quality.quarantine import Quarantine


@pytest.fixture
def q():
    return Quarantine()


class TestQuarantine:
    def test_store_adds_to_store(self, q):
        q.store("market_tick", {"symbol": "AAPL"}, reason="missing_price")
        assert len(q.get_quarantined("market_tick")) == 1

    def test_store_multiple_types(self, q):
        q.store("market_tick", {"x": 1}, "bad_price")
        q.store("news_article", {"y": 2}, "missing_title")
        assert len(q.get_quarantined("market_tick")) == 1
        assert len(q.get_quarantined("news_article")) == 1

    def test_get_quarantined_all(self, q):
        q.store("market_tick", {}, "r1")
        q.store("news_article", {}, "r2")
        all_q = q.get_quarantined()
        assert len(all_q) == 2

    def test_quarantine_entry_has_metadata(self, q):
        q.store("market_tick", {"symbol": "AAPL"}, "reason_x")
        entry = q.get_quarantined("market_tick")[0]
        assert "quarantined_at" in entry
        assert "record" in entry
        assert entry["reason"] == "reason_x"

    def test_record_total_tracking(self, q):
        q.record_total("market_tick")
        q.record_total("market_tick")
        q.store("market_tick", {}, "bad")
        report = q.quality_report()
        assert report["market_tick"]["total"] == 2
        assert report["market_tick"]["quarantined"] == 1
        assert report["market_tick"]["pass_rate"] == 0.5

    def test_quality_report_empty(self, q):
        report = q.quality_report()
        assert report == {}

    def test_summary(self, q):
        q.store("market_tick", {}, "r1")
        q.store("market_tick", {}, "r2")
        s = q.summary()
        assert s["market_tick"] == 2

    def test_persist_to_delta_called(self):
        from unittest.mock import MagicMock
        client = MagicMock()
        q = Quarantine(delta_client=client, quarantine_path="s3://bucket/quarantine")
        q.store("market_tick", {"symbol": "AAPL"}, "missing_price")
        client.write.assert_called_once()
