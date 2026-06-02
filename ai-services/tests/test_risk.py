"""Tests for RiskCalculator.

TimescaleDB access is mocked via FeatureLoader.  empyrical and scipy are
allowed to run as they are pure-math and fast.
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

from src.risk.risk_calculator import RiskCalculator


def _make_calculator_with_df(df: pd.DataFrame) -> RiskCalculator:
    calc = RiskCalculator()
    mock_loader = MagicMock()
    mock_loader.get_historical_bars.return_value = df
    calc._loader = mock_loader
    return calc


# ─── compute_metrics ─────────────────────────────────────────────────────────

class TestComputeMetrics:
    def test_returns_all_expected_keys(self, sample_ohlcv_df):
        calc = _make_calculator_with_df(sample_ohlcv_df)
        result = calc.compute_metrics("AAPL")
        for key in ("var_95", "var_99", "cvar_95", "cvar_99", "sharpe_ratio",
                    "max_drawdown", "annualized_volatility", "annualized_return",
                    "skewness", "kurtosis", "n_observations"):
            assert key in result, f"Missing key: {key}"

    def test_symbol_in_result(self, sample_ohlcv_df):
        calc = _make_calculator_with_df(sample_ohlcv_df)
        result = calc.compute_metrics("AAPL")
        assert result["symbol"] == "AAPL"

    def test_var_95_less_than_var_99(self, sample_ohlcv_df):
        calc = _make_calculator_with_df(sample_ohlcv_df)
        result = calc.compute_metrics("AAPL")
        # VaR at 95% (5th percentile) is less extreme than at 99% (1st percentile)
        assert result["var_95"] >= result["var_99"]

    def test_cvar_worse_than_var(self, sample_ohlcv_df):
        calc = _make_calculator_with_df(sample_ohlcv_df)
        result = calc.compute_metrics("AAPL")
        # CVaR (expected shortfall) should be at least as extreme as VaR
        assert result["cvar_95"] <= result["var_95"]
        assert result["cvar_99"] <= result["var_99"]

    def test_annualized_volatility_positive(self, sample_ohlcv_df):
        calc = _make_calculator_with_df(sample_ohlcv_df)
        result = calc.compute_metrics("AAPL")
        assert result["annualized_volatility"] > 0

    def test_insufficient_data_returns_error(self):
        tiny_df = pd.DataFrame({"close": [100.0] * 5})
        # Only 4 valid returns after pct_change
        calc = _make_calculator_with_df(tiny_df)
        result = calc.compute_metrics("AAPL")
        assert "error" in result

    def test_empty_df_returns_error(self):
        calc = _make_calculator_with_df(pd.DataFrame())
        result = calc.compute_metrics("AAPL")
        assert "error" in result

    def test_n_observations_matches_data(self, sample_ohlcv_df):
        calc = _make_calculator_with_df(sample_ohlcv_df)
        result = calc.compute_metrics("AAPL")
        # pct_change drops the first row
        assert result["n_observations"] == len(sample_ohlcv_df) - 1

    def test_all_values_are_finite(self, sample_ohlcv_df):
        calc = _make_calculator_with_df(sample_ohlcv_df)
        result = calc.compute_metrics("AAPL")
        numeric_keys = ("var_95", "var_99", "cvar_95", "cvar_99", "sharpe_ratio",
                        "max_drawdown", "annualized_volatility", "annualized_return")
        for k in numeric_keys:
            assert np.isfinite(result[k]), f"{k} = {result[k]} is not finite"


# ─── compute_portfolio_risk ──────────────────────────────────────────────────

class TestPortfolioRisk:
    def _calc_with_two_symbols(self, sample_ohlcv_df):
        calc = RiskCalculator()
        mock_loader = MagicMock()
        mock_loader.get_historical_bars.return_value = sample_ohlcv_df
        calc._loader = mock_loader
        return calc

    def test_portfolio_returns_var_and_sharpe(self, sample_ohlcv_df):
        calc = self._calc_with_two_symbols(sample_ohlcv_df)
        result = calc.compute_portfolio_risk([
            {"symbol": "AAPL", "weight": 0.6},
            {"symbol": "MSFT", "weight": 0.4},
        ])
        assert "var_95" in result
        assert "sharpe_ratio" in result

    def test_portfolio_normalises_weights(self, sample_ohlcv_df):
        calc = self._calc_with_two_symbols(sample_ohlcv_df)
        result = calc.compute_portfolio_risk([
            {"symbol": "AAPL", "weight": 3.0},
            {"symbol": "MSFT", "weight": 1.0},
        ])
        positions = {p["symbol"]: p["weight"] for p in result["positions"]}
        assert abs(positions["AAPL"] - 0.75) < 0.01
        assert abs(positions["MSFT"] - 0.25) < 0.01

    def test_portfolio_no_positions_returns_error(self, sample_ohlcv_df):
        calc = _make_calculator_with_df(sample_ohlcv_df)
        result = calc.compute_portfolio_risk([])
        assert "error" in result

    def test_portfolio_includes_correlation(self, sample_ohlcv_df):
        calc = self._calc_with_two_symbols(sample_ohlcv_df)
        result = calc.compute_portfolio_risk([
            {"symbol": "AAPL", "weight": 0.5},
            {"symbol": "MSFT", "weight": 0.5},
        ])
        assert "correlation" in result


# ─── backtest ────────────────────────────────────────────────────────────────

class TestBacktest:
    def test_backtest_returns_expected_keys(self, sample_ohlcv_df):
        calc = _make_calculator_with_df(sample_ohlcv_df)
        result = calc.backtest("AAPL")
        for key in ("symbol", "strategy", "total_return", "sharpe_ratio",
                    "max_drawdown", "annualized_volatility", "n_periods"):
            assert key in result

    def test_backtest_symbol_attached(self, sample_ohlcv_df):
        calc = _make_calculator_with_df(sample_ohlcv_df)
        result = calc.backtest("MSFT")
        assert result["symbol"] == "MSFT"

    def test_backtest_strategy_attached(self, sample_ohlcv_df):
        calc = _make_calculator_with_df(sample_ohlcv_df)
        result = calc.backtest("AAPL", strategy="buy_hold")
        assert result["strategy"] == "buy_hold"

    def test_backtest_empty_data_returns_error(self):
        calc = _make_calculator_with_df(pd.DataFrame())
        result = calc.backtest("AAPL")
        assert "error" in result

    def test_backtest_total_return_is_float(self, sample_ohlcv_df):
        calc = _make_calculator_with_df(sample_ohlcv_df)
        result = calc.backtest("AAPL")
        assert isinstance(result["total_return"], float)


# ─── VaR / CVaR math correctness ─────────────────────────────────────────────

class TestVaRMath:
    """Verify VaR/CVaR math against known-correct values."""

    def test_var_95_is_5th_percentile(self, sample_returns):
        calc = RiskCalculator()
        r = sample_returns.values
        expected_var = float(np.percentile(r, 5))
        metrics = calc._metrics_from_returns("TEST", sample_returns)
        # result is rounded to 6dp, so tolerance of 1e-5 is appropriate
        assert abs(metrics["var_95"] - expected_var) < 1e-5

    def test_cvar_95_leq_var_95(self, sample_returns):
        calc = RiskCalculator()
        metrics = calc._metrics_from_returns("TEST", sample_returns)
        assert metrics["cvar_95"] <= metrics["var_95"]

    def test_max_drawdown_negative(self, sample_returns):
        calc = RiskCalculator()
        metrics = calc._metrics_from_returns("TEST", sample_returns)
        assert metrics["max_drawdown"] <= 0
