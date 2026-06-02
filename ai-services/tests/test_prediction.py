"""Tests for XGBPredictor and the build_features function.

XGBoost, Redis, and PostgreSQL are all mocked.
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

from src.prediction.xgb_predictor import XGBPredictor, build_features, _FEATURE_COLS


# ─── build_features ──────────────────────────────────────────────────────────

class TestBuildFeatures:
    def test_returns_correct_shapes(self, sample_ohlcv_df):
        X, y = build_features(sample_ohlcv_df)
        assert X.ndim == 2
        assert y.ndim == 1
        assert X.shape[1] == len(_FEATURE_COLS)
        assert len(X) == len(y)

    def test_samples_less_than_threshold_returns_empty(self):
        from src.config import settings
        tiny_df = pd.DataFrame({
            "timestamp": pd.date_range("2024-01-01", periods=5, freq="1min"),
            "open": [100.0] * 5,
            "high": [101.0] * 5,
            "low": [99.0] * 5,
            "close": [100.5] * 5,
            "volume": [1e6] * 5,
            "vwap": [100.25] * 5,
        })
        X, y = build_features(tiny_df)
        assert len(X) == 0

    def test_feature_columns_are_finite(self, sample_ohlcv_df):
        X, y = build_features(sample_ohlcv_df)
        assert np.isfinite(X).all(), "Features contain NaN or Inf"

    def test_target_is_binary(self, sample_ohlcv_df):
        X, y = build_features(sample_ohlcv_df)
        unique_vals = set(y)
        assert unique_vals <= {0, 1}

    def test_feature_names_match(self):
        assert _FEATURE_COLS == ["return_1", "hl_ratio", "oc_ratio", "vol_z", "vwap_spread"]

    def test_hl_ratio_non_negative(self, sample_ohlcv_df):
        X, y = build_features(sample_ohlcv_df)
        hl_idx = _FEATURE_COLS.index("hl_ratio")
        assert (X[:, hl_idx] >= 0).all()


# ─── XGBPredictor ────────────────────────────────────────────────────────────

class TestXGBPredictor:
    def _predictor_with_mock_loader(self, features=None, df=None):
        pred = XGBPredictor()
        mock_loader = MagicMock()
        mock_loader.get_latest_features.return_value = features
        mock_loader.get_historical_bars.return_value = df if df is not None else pd.DataFrame()
        pred._loader = mock_loader
        return pred

    def _install_mock_model(self, pred: XGBPredictor, up_prob: float = 0.7):
        mock_model = MagicMock()
        mock_model.predict_proba.return_value = np.array([[1 - up_prob, up_prob]])
        pred._models["AAPL"] = mock_model
        pred._last_trained["AAPL"] = float("inf")  # prevent retrain
        return mock_model

    # ─── predict ─────────────────────────────────────────────────

    def test_predict_up_when_high_probability(self, sample_ohlcv_df):
        pred = self._predictor_with_mock_loader(
            features={"open": 100.0, "high": 102.0, "low": 99.0, "close": 101.5, "volume": 1e6, "vwap": 100.5}
        )
        self._install_mock_model(pred, up_prob=0.75)
        result = pred.predict("AAPL")
        assert result["symbol"] == "AAPL"
        assert result["direction"] == "up"
        assert result["confidence"] > 0.5
        assert result["horizon"] == "1hr"

    def test_predict_down_when_low_probability(self):
        pred = self._predictor_with_mock_loader(
            features={"open": 100.0, "high": 101.0, "low": 98.0, "close": 99.0, "volume": 1e6, "vwap": 99.5}
        )
        self._install_mock_model(pred, up_prob=0.3)
        result = pred.predict("AAPL")
        assert result["direction"] == "down"

    def test_predict_neutral_when_near_50(self):
        pred = self._predictor_with_mock_loader(
            features={"open": 100.0, "high": 101.0, "low": 99.0, "close": 100.0, "volume": 1e6, "vwap": 100.0}
        )
        self._install_mock_model(pred, up_prob=0.50)
        result = pred.predict("AAPL")
        assert result["direction"] == "neutral"

    def test_predict_no_features_returns_neutral(self):
        pred = self._predictor_with_mock_loader(features=None)
        pred._last_trained["AAPL"] = float("inf")
        result = pred.predict("AAPL")
        assert result["direction"] == "neutral"
        assert result["error"] == "no_features"

    def test_predict_no_model_returns_neutral(self):
        pred = self._predictor_with_mock_loader(
            features={"open": 100.0, "high": 101.0, "low": 99.0, "close": 100.0, "volume": 1e6, "vwap": 100.0}
        )
        pred._last_trained["AAPL"] = float("inf")
        result = pred.predict("AAPL")
        assert result["direction"] == "neutral"
        assert result["error"] == "no_model"

    def test_predict_returns_up_probability(self):
        pred = self._predictor_with_mock_loader(
            features={"open": 100.0, "high": 102.0, "low": 99.0, "close": 101.0, "volume": 1e6, "vwap": 100.5}
        )
        self._install_mock_model(pred, up_prob=0.68)
        result = pred.predict("AAPL")
        assert abs(result["up_probability"] - 0.68) < 0.001

    # ─── batch predict ───────────────────────────────────────────

    def test_predict_batch(self):
        pred = self._predictor_with_mock_loader(features=None)
        pred._last_trained["AAPL"] = float("inf")
        pred._last_trained["MSFT"] = float("inf")
        results = pred.predict_batch(["AAPL", "MSFT"])
        assert len(results) == 2
        assert results[0]["symbol"] == "AAPL"
        assert results[1]["symbol"] == "MSFT"

    # ─── train ───────────────────────────────────────────────────

    def test_train_returns_false_when_no_data(self):
        pred = self._predictor_with_mock_loader(df=pd.DataFrame())
        result = pred.train("AAPL")
        assert result is False

    def test_train_returns_true_with_sufficient_data(self, sample_ohlcv_df):
        import sys

        pred = XGBPredictor()
        mock_loader = MagicMock()
        mock_loader.get_historical_bars.return_value = sample_ohlcv_df
        pred._loader = mock_loader

        mock_xgb_instance = MagicMock()
        mock_xgb_module = MagicMock()
        mock_xgb_module.XGBClassifier.return_value = mock_xgb_instance
        with patch.dict(sys.modules, {"xgboost": mock_xgb_module}):
            result = pred.train("AAPL")

        assert result is True
        assert "AAPL" in pred._models
        mock_xgb_instance.fit.assert_called_once()

    def test_train_stores_trained_model(self, sample_ohlcv_df):
        import sys

        pred = XGBPredictor()
        mock_loader = MagicMock()
        mock_loader.get_historical_bars.return_value = sample_ohlcv_df
        pred._loader = mock_loader

        mock_xgb_instance = MagicMock()
        mock_xgb_module = MagicMock()
        mock_xgb_module.XGBClassifier.return_value = mock_xgb_instance
        with patch.dict(sys.modules, {"xgboost": mock_xgb_module}):
            pred.train("TSLA")

        assert pred._models["TSLA"] is mock_xgb_instance

    # ─── shap ────────────────────────────────────────────────────

    def test_shap_no_model(self):
        pred = self._predictor_with_mock_loader()
        result = pred.get_shap_values("AAPL")
        assert result["error"] == "no_model"
        assert result["shap_values"] == {}
