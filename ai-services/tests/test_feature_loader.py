"""Tests for FeatureLoader.

Redis and psycopg2 are fully mocked — no live services needed.
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src.prediction.feature_loader import FeatureLoader


class TestFeatureLoader:
    # ─── get_latest_features ─────────────────────────────────────

    def test_returns_dict_when_redis_has_data(self):
        loader = FeatureLoader()
        mock_redis = MagicMock()
        mock_redis.hgetall.return_value = {
            "open": "150.0",
            "close": "152.5",
            "volume": "1000000",
        }
        loader._redis = mock_redis

        result = loader.get_latest_features("AAPL")
        assert result is not None
        assert result["close"] == 152.5
        assert result["volume"] == 1_000_000.0

    def test_returns_none_when_redis_empty(self):
        loader = FeatureLoader()
        mock_redis = MagicMock()
        mock_redis.hgetall.return_value = {}
        loader._redis = mock_redis

        result = loader.get_latest_features("AAPL")
        assert result is None

    def test_redis_key_pattern(self):
        loader = FeatureLoader()
        mock_redis = MagicMock()
        mock_redis.hgetall.return_value = {}
        loader._redis = mock_redis

        loader.get_latest_features("MSFT")
        mock_redis.hgetall.assert_called_once_with("finstreami:features:MSFT")

    def test_non_numeric_values_kept_as_string(self):
        loader = FeatureLoader()
        mock_redis = MagicMock()
        mock_redis.hgetall.return_value = {"updated_ms": "not_a_float_but_string"}
        loader._redis = mock_redis

        result = loader.get_latest_features("AAPL")
        # non-convertible values are kept as-is
        assert result["updated_ms"] == "not_a_float_but_string"

    def test_all_numeric_values_converted_to_float(self):
        loader = FeatureLoader()
        mock_redis = MagicMock()
        mock_redis.hgetall.return_value = {"open": "100", "close": "101.5"}
        loader._redis = mock_redis

        result = loader.get_latest_features("AAPL")
        assert isinstance(result["open"], float)
        assert isinstance(result["close"], float)

    # ─── get_historical_bars ─────────────────────────────────────

    def test_returns_dataframe_with_correct_columns(self):
        loader = FeatureLoader()

        mock_row = {
            "timestamp": "2024-01-01 09:30:00",
            "open": 150.0,
            "high": 152.0,
            "low": 149.5,
            "close": 151.5,
            "volume": 1_000_000,
            "vwap": 150.8,
        }

        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_cur.__enter__ = MagicMock(return_value=mock_cur)
        mock_cur.__exit__ = MagicMock(return_value=False)
        mock_cur.fetchall.return_value = [mock_row]

        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value = mock_cur

        with patch("psycopg2.connect", return_value=mock_conn), \
             patch("psycopg2.extras.DictCursor"):
            df = loader.get_historical_bars("AAPL", limit=10)

        assert isinstance(df, pd.DataFrame)
        assert set(df.columns) == {"timestamp", "open", "high", "low", "close", "volume", "vwap"}

    def test_returns_empty_df_when_db_unavailable(self):
        loader = FeatureLoader()
        with patch("psycopg2.connect", side_effect=Exception("connection refused")):
            df = loader.get_historical_bars("AAPL")
        assert df.empty

    def test_returns_empty_df_when_no_rows(self):
        loader = FeatureLoader()

        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_cur.__enter__ = MagicMock(return_value=mock_cur)
        mock_cur.__exit__ = MagicMock(return_value=False)
        mock_cur.fetchall.return_value = []

        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value = mock_cur

        with patch("psycopg2.connect", return_value=mock_conn), \
             patch("psycopg2.extras.DictCursor"):
            df = loader.get_historical_bars("AAPL")

        assert df.empty

    # ─── lazy loading ─────────────────────────────────────────────

    def test_redis_lazy_loaded(self):
        loader = FeatureLoader()
        assert loader._redis is None
