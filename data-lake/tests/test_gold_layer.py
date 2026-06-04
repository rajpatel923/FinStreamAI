"""Tests for GoldLayer."""
from __future__ import annotations

import pandas as pd
import pytest


class TestGoldLayer:
    def _market_df(self):
        return pd.DataFrame([
            {"symbol": "AAPL", "price": 150.0, "timestamp": "2023-11-14T22:00:00"},
            {"symbol": "AAPL", "price": 155.0, "timestamp": "2023-11-14T22:01:00"},
            {"symbol": "MSFT", "price": 300.0, "timestamp": "2023-11-14T22:00:00"},
            {"symbol": "MSFT", "price": 305.0, "timestamp": "2023-11-14T22:01:00"},
        ])

    def test_build_features_computes_stats(self, gold_layer, mock_delta_client):
        mock_delta_client.read.return_value = self._market_df()
        df = gold_layer.build_features()
        assert not df.empty
        assert "price_mean" in df.columns
        assert "price_std" in df.columns

    def test_build_features_no_silver_data(self, gold_layer, mock_delta_client):
        mock_delta_client.table_exists.return_value = False
        df = gold_layer.build_features()
        assert df.empty

    def test_build_features_attaches_sentiment(self, gold_layer, mock_delta_client):
        news_df = pd.DataFrame([
            {"symbol": "AAPL", "sentiment_score": 0.8},
            {"symbol": "AAPL", "sentiment_score": 0.4},
        ])

        def _read(path):
            if "market_tick" in path:
                return self._market_df()
            if "news_article" in path:
                return news_df
            return pd.DataFrame()

        mock_delta_client.read.side_effect = _read
        df = gold_layer.build_features()
        assert "avg_sentiment" in df.columns
        aapl = df[df["symbol"] == "AAPL"]
        assert abs(aapl["avg_sentiment"].iloc[0] - 0.6) < 0.01

    def test_build_signals_computes_momentum(self, gold_layer, mock_delta_client):
        mock_delta_client.read.return_value = self._market_df()
        df = gold_layer.build_signals()
        assert not df.empty
        assert "momentum" in df.columns
        assert "volatility" in df.columns

    def test_build_signals_no_data(self, gold_layer, mock_delta_client):
        mock_delta_client.table_exists.return_value = False
        df = gold_layer.build_signals()
        assert df.empty

    def test_build_signals_single_tick(self, gold_layer, mock_delta_client):
        mock_delta_client.read.return_value = pd.DataFrame([
            {"symbol": "AAPL", "price": 150.0},
        ])
        df = gold_layer.build_signals()
        # Single tick — not enough for momentum
        assert df.empty

    def test_build_analytics_news_volume(self, gold_layer, mock_delta_client):
        news_df = pd.DataFrame([
            {"symbol": "AAPL", "sentiment_score": 0.5},
            {"symbol": "AAPL", "sentiment_score": 0.3},
            {"symbol": "MSFT", "sentiment_score": -0.1},
        ])

        def _read(path):
            if "news_article" in path:
                return news_df
            return pd.DataFrame()

        mock_delta_client.read.side_effect = _read
        df = gold_layer.build_analytics()
        assert not df.empty
        aapl = df[df["symbol"] == "AAPL"]
        assert aapl["news_count"].iloc[0] == 2

    def test_build_analytics_no_data(self, gold_layer, mock_delta_client):
        mock_delta_client.table_exists.return_value = False
        df = gold_layer.build_analytics()
        assert df.empty
