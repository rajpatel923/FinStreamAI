from __future__ import annotations

import numpy as np
import pandas as pd
import structlog

from src.prediction.feature_loader import FeatureLoader

logger = structlog.get_logger(__name__)


class RiskCalculator:
    """Financial risk metrics computed from historical price data.

    All metrics use closed-form math or well-established libraries
    (empyrical, scipy) — no ML model training required.

    Metrics computed:
        VaR 95/99 (Historical Simulation)
        CVaR / Expected Shortfall 95/99
        Sharpe Ratio (annualised, risk-free rate = 0)
        Max Drawdown
        Annualised Volatility & Return
        Skewness / Excess Kurtosis
    """

    def __init__(self) -> None:
        self._loader = FeatureLoader()

    # ─── Single-asset metrics ─────────────────────────────────────

    def compute_metrics(self, symbol: str) -> dict:
        """Full risk metrics for a single symbol."""
        df = self._loader.get_historical_bars(symbol, limit=252)
        if df.empty or len(df) < 20:
            return {"symbol": symbol, "error": "insufficient_data", "n_observations": len(df)}
        returns = df["close"].pct_change().dropna()
        return self._metrics_from_returns(symbol, returns)

    def _metrics_from_returns(self, symbol: str, returns: pd.Series) -> dict:
        from scipy import stats

        r = returns.values.astype(float)

        # ── VaR (Historical Simulation) ──
        var_95 = float(np.percentile(r, 5))
        var_99 = float(np.percentile(r, 1))

        # ── CVaR / Expected Shortfall ──
        mask_95 = r <= var_95
        cvar_95 = float(r[mask_95].mean()) if mask_95.any() else var_95
        mask_99 = r <= var_99
        cvar_99 = float(r[mask_99].mean()) if mask_99.any() else var_99

        # ── Sharpe (annualised, rf=0) ──
        sharpe = self._safe_sharpe(returns)

        # ── Max Drawdown ──
        max_dd = self._safe_max_drawdown(returns)

        # ── Annualised stats ──
        ann_vol = float(np.std(r, ddof=1) * np.sqrt(252))
        mean_daily = float(np.mean(r))
        ann_return = float((1.0 + mean_daily) ** 252 - 1.0)

        # ── Higher moments ──
        skew = float(stats.skew(r))
        kurt = float(stats.kurtosis(r))  # excess kurtosis

        return {
            "symbol": symbol,
            "var_95": round(var_95, 6),
            "var_99": round(var_99, 6),
            "cvar_95": round(cvar_95, 6),
            "cvar_99": round(cvar_99, 6),
            "sharpe_ratio": round(sharpe, 4),
            "max_drawdown": round(max_dd, 4),
            "annualized_volatility": round(ann_vol, 4),
            "annualized_return": round(ann_return, 4),
            "mean_daily_return": round(mean_daily, 6),
            "skewness": round(skew, 4),
            "kurtosis": round(kurt, 4),
            "n_observations": len(r),
        }

    # ─── Portfolio metrics ────────────────────────────────────────

    def compute_portfolio_risk(self, positions: list[dict]) -> dict:
        """Portfolio-level risk given a list of weighted positions.

        Args:
            positions: [{"symbol": "AAPL", "weight": 0.5}, ...]
                       Weights are normalised to sum=1 if they don't already.
        """
        if not positions:
            return {"error": "no_positions"}

        # Normalise weights
        total = sum(p.get("weight", 0.0) for p in positions)
        if total <= 0:
            return {"error": "invalid_weights"}
        for p in positions:
            p["weight"] = p.get("weight", 0.0) / total

        returns_map: dict[str, pd.Series] = {}
        for pos in positions:
            df = self._loader.get_historical_bars(pos["symbol"], limit=252)
            if not df.empty:
                returns_map[pos["symbol"]] = df["close"].pct_change().dropna()

        if not returns_map:
            return {"error": "no_data_for_any_symbol"}

        returns_df = pd.DataFrame(returns_map).dropna()
        if returns_df.empty:
            return {"error": "no_aligned_returns"}

        weights = np.array([
            next((p["weight"] for p in positions if p["symbol"] == sym), 0.0)
            for sym in returns_df.columns
        ])
        port_returns = pd.Series(
            returns_df.values @ weights,
            index=returns_df.index,
        )

        metrics = self._metrics_from_returns("PORTFOLIO", port_returns)
        metrics["positions"] = [
            {"symbol": p["symbol"], "weight": round(p["weight"], 4)}
            for p in positions
            if p["symbol"] in returns_df.columns
        ]
        corr = returns_df.corr()
        metrics["correlation"] = {
            s1: {s2: round(corr.loc[s1, s2], 4) for s2 in corr.columns}
            for s1 in corr.index
        }
        return metrics

    # ─── Backtest ────────────────────────────────────────────────

    def backtest(self, symbol: str, strategy: str = "buy_hold") -> dict:
        """Simple buy-and-hold backtest over available history."""
        df = self._loader.get_historical_bars(symbol, limit=252)
        if df.empty or len(df) < 5:
            return {"symbol": symbol, "strategy": strategy, "error": "insufficient_data"}

        returns = df["close"].pct_change().dropna()
        cumulative = (1.0 + returns).cumprod()
        total_return = float(cumulative.iloc[-1] - 1.0)

        return {
            "symbol": symbol,
            "strategy": strategy,
            "total_return": round(total_return, 4),
            "sharpe_ratio": round(self._safe_sharpe(returns), 4),
            "max_drawdown": round(self._safe_max_drawdown(returns), 4),
            "annualized_volatility": round(float(returns.std(ddof=1) * np.sqrt(252)), 4),
            "n_periods": len(returns),
        }

    # ─── Helpers ────────────────────────────────────────────────

    @staticmethod
    def _safe_sharpe(returns: pd.Series) -> float:
        try:
            import empyrical
            return float(empyrical.sharpe_ratio(returns))
        except Exception:
            std = returns.std()
            if std == 0:
                return 0.0
            return float(returns.mean() / std * np.sqrt(252))

    @staticmethod
    def _safe_max_drawdown(returns: pd.Series) -> float:
        try:
            import empyrical
            return float(empyrical.max_drawdown(returns))
        except Exception:
            cumulative = (1.0 + returns).cumprod()
            running_max = cumulative.cummax()
            drawdown = (cumulative - running_max) / running_max
            return float(drawdown.min())
