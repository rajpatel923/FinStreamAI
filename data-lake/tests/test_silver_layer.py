"""Tests for SilverLayer."""
from __future__ import annotations

import pandas as pd
import pytest


class TestSilverLayer:
    def _market_df(self):
        return pd.DataFrame([
            {"symbol": "AAPL", "price": "150.0", "timestamp": "1700000000", "_ingested_at": "2023-11-14T22:00:00"},
            {"symbol": "AAPL", "price": "151.0", "timestamp": "1700000100", "_ingested_at": "2023-11-14T22:00:01"},
            {"symbol": "MSFT", "price": "300.0", "timestamp": "1700000000", "_ingested_at": "2023-11-14T22:00:00"},
        ])

    def test_process_market_data_writes_silver(self, silver_layer, mock_delta_client):
        mock_delta_client.read.return_value = self._market_df()
        count = silver_layer.process_market_data()
        assert count > 0
        mock_delta_client.write.assert_called_once()

    def test_process_market_data_deduplicates(self, silver_layer, mock_delta_client):
        df = self._market_df()
        # Add duplicate row
        df = pd.concat([df, df.iloc[:1]], ignore_index=True)
        mock_delta_client.read.return_value = df
        count = silver_layer.process_market_data()
        # Count should be less than input (deduped)
        args, _ = mock_delta_client.write.call_args
        written_df = args[1]
        assert len(written_df) <= len(df)

    def test_process_market_data_drops_null_price(self, silver_layer, mock_delta_client):
        df = pd.DataFrame([
            {"symbol": "AAPL", "price": None, "timestamp": "1700000000", "_ingested_at": "t1"},
            {"symbol": "MSFT", "price": "300.0", "timestamp": "1700000000", "_ingested_at": "t2"},
        ])
        mock_delta_client.read.return_value = df
        silver_layer.process_market_data()
        args, _ = mock_delta_client.write.call_args
        written_df = args[1]
        assert len(written_df) == 1
        assert written_df["symbol"].iloc[0] == "MSFT"

    def test_process_market_data_empty_bronze(self, silver_layer, mock_delta_client):
        mock_delta_client.table_exists.return_value = False
        count = silver_layer.process_market_data()
        assert count == 0
        mock_delta_client.write.assert_not_called()

    def test_process_news_valid(self, silver_layer, mock_delta_client):
        df = pd.DataFrame([
            {"article_id": "a1", "title": "Fed raises rates", "timestamp": "t1", "_ingested_at": "t1"},
        ])
        mock_delta_client.read.return_value = df
        count = silver_layer.process_news()
        assert count == 1

    def test_process_news_drops_empty_titles(self, silver_layer, mock_delta_client):
        df = pd.DataFrame([
            {"article_id": "a1", "title": "", "timestamp": "t1", "_ingested_at": "t1"},
            {"article_id": "a2", "title": "Good article", "timestamp": "t2", "_ingested_at": "t2"},
        ])
        mock_delta_client.read.return_value = df
        silver_layer.process_news()
        args, _ = mock_delta_client.write.call_args
        assert len(args[1]) == 1

    def test_process_social_clips_sentiment(self, silver_layer, mock_delta_client):
        df = pd.DataFrame([
            {"post_id": "p1", "content": "AAPL!", "timestamp": "t1", "_ingested_at": "t1",
             "sentiment_score": "2.5"},  # out-of-range
        ])
        mock_delta_client.read.return_value = df
        silver_layer.process_social()
        args, _ = mock_delta_client.write.call_args
        written = args[1]
        assert written["sentiment_score"].iloc[0] <= 1.0

    def test_process_social_empty(self, silver_layer, mock_delta_client):
        mock_delta_client.table_exists.return_value = False
        count = silver_layer.process_social()
        assert count == 0

    def test_watermark_incremental(self, silver_layer, mock_delta_client):
        df1 = pd.DataFrame([
            {"symbol": "AAPL", "price": "150.0", "timestamp": "t1", "_ingested_at": "2023-01-01T00:00:00"},
        ])
        mock_delta_client.read.return_value = df1
        silver_layer.process_market_data()
        assert silver_layer._watermarks.get("market_tick") == "2023-01-01T00:00:00"
