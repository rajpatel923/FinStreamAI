"""Tests for BronzeLayer."""
from __future__ import annotations

import pytest
import pandas as pd
from unittest.mock import MagicMock, call


class TestBronzeLayer:
    def test_write_market_tick_valid(self, bronze_layer, mock_delta_client):
        record = {"symbol": "AAPL", "price": 150.0, "timestamp": 1700000000, "volume": 1000}
        bronze_layer.write_market_tick(record)
        mock_delta_client.write.assert_called_once()
        args, kwargs = mock_delta_client.write.call_args
        assert "market_tick" in args[0]
        df = args[1]
        assert "_ingested_at" in df.columns
        assert "_source_system" in df.columns
        assert df["_source_system"].iloc[0] == "kafka"

    def test_write_market_tick_missing_required(self, bronze_layer, mock_delta_client, quarantine):
        record = {"symbol": "AAPL", "timestamp": 1700000000}  # missing price
        bronze_layer.write_market_tick(record)
        mock_delta_client.write.assert_not_called()
        assert len(quarantine.get_quarantined("market_tick")) == 1

    def test_write_news_article_valid(self, bronze_layer, mock_delta_client):
        record = {"article_id": "a1", "title": "Test", "timestamp": 1700000000}
        bronze_layer.write_news_article(record)
        mock_delta_client.write.assert_called_once()
        args, _ = mock_delta_client.write.call_args
        assert "news_article" in args[0]

    def test_write_news_article_missing_required(self, bronze_layer, mock_delta_client, quarantine):
        record = {"article_id": "a1", "timestamp": 1700000000}  # missing title
        bronze_layer.write_news_article(record)
        mock_delta_client.write.assert_not_called()

    def test_write_social_post_valid(self, bronze_layer, mock_delta_client):
        record = {"post_id": "p1", "content": "AAPL to the moon!", "timestamp": 1700000000}
        bronze_layer.write_social_post(record)
        mock_delta_client.write.assert_called_once()

    def test_write_event_valid(self, bronze_layer, mock_delta_client):
        record = {"event_id": "e1", "event_type": "earnings", "timestamp": 1700000000}
        bronze_layer.write_event(record)
        mock_delta_client.write.assert_called_once()
        args, _ = mock_delta_client.write.call_args
        assert "event" in args[0]

    def test_partitions_include_time(self, bronze_layer, mock_delta_client):
        record = {"symbol": "MSFT", "price": 300.0, "timestamp": 1700000000}
        bronze_layer.write_market_tick(record)
        _, kwargs = mock_delta_client.write.call_args
        partition_by = kwargs.get("partition_by") or mock_delta_client.write.call_args[0][3] if len(mock_delta_client.write.call_args[0]) > 3 else None
        # Check that partition_by was passed (either positional or keyword)
        call_kwargs = mock_delta_client.write.call_args[1]
        assert "partition_by" in call_kwargs or len(mock_delta_client.write.call_args[0]) >= 4

    def test_time_parts_from_unix_timestamp(self, bronze_layer):
        import datetime
        parts = bronze_layer._time_parts(1700000000)
        assert "year" in parts
        assert "month" in parts
        assert "day" in parts
        assert "hour" in parts

    def test_time_parts_from_string(self, bronze_layer):
        parts = bronze_layer._time_parts("2023-11-14T22:13:20")
        assert parts["year"] == 2023

    def test_time_parts_from_datetime(self, bronze_layer):
        import datetime
        dt = datetime.datetime(2023, 6, 1, 12, 0, 0)
        parts = bronze_layer._time_parts(dt)
        assert parts["year"] == 2023
        assert parts["month"] == 6

    def test_enrich_adds_metadata(self, bronze_layer):
        record = {"symbol": "AAPL", "price": 100.0}
        enriched = bronze_layer._enrich(record)
        assert "_ingested_at" in enriched
        assert "_source_system" in enriched
        assert enriched["_source_system"] == "kafka"
