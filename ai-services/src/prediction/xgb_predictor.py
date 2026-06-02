from __future__ import annotations

import threading
import time

import numpy as np
import pandas as pd
import structlog

from src.config import settings
from src.prediction.feature_loader import FeatureLoader

logger = structlog.get_logger(__name__)

_FEATURE_COLS = ["return_1", "hl_ratio", "oc_ratio", "vol_z", "vwap_spread"]


def build_features(df: pd.DataFrame) -> tuple[np.ndarray, np.ndarray]:
    """Engineer features and directional targets from raw OHLCV data.

    Features:
        return_1    — 1-bar log return
        hl_ratio    — (high - low) / close  (intrabar range)
        oc_ratio    — (close - open) / close (bar direction)
        vol_z       — volume z-score over 20-bar rolling window
        vwap_spread — (close - vwap) / close

    Target:
        1 if close[t+1] > close[t], else 0  (binary direction)
    """
    df = df.copy()
    df["return_1"] = df["close"].pct_change()
    df["hl_ratio"] = (df["high"] - df["low"]) / df["close"].clip(lower=1e-8)
    df["oc_ratio"] = (df["close"] - df["open"]) / df["close"].clip(lower=1e-8)
    df["vol_z"] = (df["volume"] - df["volume"].rolling(20).mean()) / (
        df["volume"].rolling(20).std() + 1e-8
    )
    df["vwap_spread"] = (df["close"] - df["vwap"]) / df["close"].clip(lower=1e-8)
    df["target"] = (df["close"].shift(-1) > df["close"]).astype(int)
    df = df.dropna()

    if len(df) < settings.AI_MIN_TRAIN_SAMPLES + 1:
        return np.empty((0, len(_FEATURE_COLS))), np.empty(0)

    # Drop the last row (target is unknown for the final bar)
    X = df[_FEATURE_COLS].values[:-1]
    y = df["target"].values[:-1]
    return X, y


class XGBPredictor:
    """Rolling XGBoost classifier: trains at startup, retrains hourly.

    One model per symbol.  All model access is protected by a lock so the
    retrain thread can swap models without blocking prediction for too long.
    """

    def __init__(self) -> None:
        self._models: dict[str, object] = {}
        self._last_trained: dict[str, float] = {}
        self._loader = FeatureLoader()
        self._lock = threading.Lock()

    # ─── Training ────────────────────────────────────────────────

    def train(self, symbol: str) -> bool:
        """Train (or retrain) the XGBoost model for a symbol.

        Returns True if training succeeded, False if data was insufficient.
        """
        df = self._loader.get_historical_bars(symbol, limit=500)
        if df.empty:
            logger.warning("No historical bars for training", symbol=symbol)
            return False

        X, y = build_features(df)
        if len(X) < settings.AI_MIN_TRAIN_SAMPLES:
            logger.warning("Insufficient samples", symbol=symbol, n_samples=len(X))
            return False

        from xgboost import XGBClassifier  # lazy import — only when we have enough data

        model = XGBClassifier(
            n_estimators=100,
            max_depth=4,
            learning_rate=0.05,
            subsample=0.8,
            colsample_bytree=0.8,
            eval_metric="logloss",
            verbosity=0,
        )
        model.fit(X, y)

        with self._lock:
            self._models[symbol] = model
            self._last_trained[symbol] = time.time()

        logger.info("XGBoost model trained", symbol=symbol, n_samples=len(X))
        return True

    def _needs_retrain(self, symbol: str) -> bool:
        last = self._last_trained.get(symbol, 0)
        return time.time() - last > settings.XGB_RETRAIN_INTERVAL_S

    # ─── Prediction ──────────────────────────────────────────────

    def predict(self, symbol: str) -> dict:
        """Predict directional signal for a symbol using the latest Redis features.

        Returns {"symbol", "direction", "confidence", "up_probability", "horizon"}.
        If the model is missing or features unavailable, direction is "neutral".
        """
        if self._needs_retrain(symbol):
            self.train(symbol)

        features = self._loader.get_latest_features(symbol)
        if features is None:
            return _no_signal(symbol, "no_features")

        with self._lock:
            model = self._models.get(symbol)
        if model is None:
            return _no_signal(symbol, "no_model")

        try:
            close = features.get("close", 0.0)
            open_ = features.get("open", close)
            high = features.get("high", close)
            low = features.get("low", close)
            volume = features.get("volume", 0.0)
            vwap = features.get("vwap", close) or close

            return_1 = (close - open_) / max(abs(open_), 1e-8)
            hl_ratio = (high - low) / max(close, 1e-8)
            oc_ratio = (close - open_) / max(close, 1e-8)
            vol_z = 0.0  # single-point snapshot; rolling z not available from Redis
            vwap_spread = (close - vwap) / max(close, 1e-8)

            X = np.array([[return_1, hl_ratio, oc_ratio, vol_z, vwap_spread]])
            proba = model.predict_proba(X)[0]
            up_prob = float(proba[1])

            if up_prob > 0.55:
                direction = "up"
            elif up_prob < 0.45:
                direction = "down"
            else:
                direction = "neutral"

            return {
                "symbol": symbol,
                "direction": direction,
                "confidence": round(max(up_prob, 1.0 - up_prob), 4),
                "up_probability": round(up_prob, 4),
                "horizon": "1hr",
            }
        except Exception as exc:
            logger.error("Prediction error", symbol=symbol, error=str(exc))
            return _no_signal(symbol, str(exc))

    def predict_batch(self, symbols: list[str]) -> list[dict]:
        return [self.predict(s) for s in symbols]

    def get_shap_values(self, symbol: str) -> dict:
        """Return SHAP feature importances for the symbol's current model."""
        with self._lock:
            model = self._models.get(symbol)
        if model is None:
            return {"symbol": symbol, "shap_values": {}, "error": "no_model"}
        try:
            import shap

            explainer = shap.TreeExplainer(model)
            X_dummy = np.zeros((1, len(_FEATURE_COLS)))
            shap_vals = explainer.shap_values(X_dummy)[0]
            return {
                "symbol": symbol,
                "shap_values": dict(zip(_FEATURE_COLS, shap_vals.tolist())),
            }
        except Exception as exc:
            logger.error("SHAP error", symbol=symbol, error=str(exc))
            return {"symbol": symbol, "shap_values": {}, "error": str(exc)}

    # ─── Background retrain scheduler ────────────────────────────

    def start_retrain_scheduler(self) -> None:
        """Spawn a daemon thread that retrains all watched symbols every hour."""

        def _loop() -> None:
            while True:
                time.sleep(settings.XGB_RETRAIN_INTERVAL_S)
                for sym in settings.watched_symbols_list:
                    try:
                        self.train(sym)
                    except Exception as exc:
                        logger.error("Scheduled retrain failed", symbol=sym, error=str(exc))

        t = threading.Thread(target=_loop, name="xgb-retrain", daemon=True)
        t.start()
        logger.info("XGBoost retrain scheduler started", interval_s=settings.XGB_RETRAIN_INTERVAL_S)


def _no_signal(symbol: str, reason: str) -> dict:
    return {
        "symbol": symbol,
        "direction": "neutral",
        "confidence": 0.0,
        "up_probability": 0.5,
        "horizon": "1hr",
        "error": reason,
    }
