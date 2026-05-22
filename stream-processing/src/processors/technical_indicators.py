import math
import uuid
from collections import deque

from src.state.rolling_state import SymbolState


class TechnicalIndicators:
    """
    Incremental technical indicator computations from SymbolState.
    All methods are stateless helpers — state is stored in SymbolState.
    """

    # ─── SMA ────────────────────────────────────────────────────
    @staticmethod
    def sma(prices: deque, n: int) -> float | None:
        if len(prices) < n:
            return None
        recent = list(prices)[-n:]
        return sum(recent) / n

    # ─── EMA (incremental, Wilder-style k = 2/(N+1)) ────────────
    @staticmethod
    def ema(price: float, prev_ema: float | None, n: int) -> float:
        k = 2.0 / (n + 1)
        if prev_ema is None:
            return price
        return price * k + prev_ema * (1.0 - k)

    # ─── RSI (14) ───────────────────────────────────────────────
    @staticmethod
    def rsi(gains: deque, losses: deque, period: int = 14) -> float | None:
        if len(gains) < period:
            return None
        g = list(gains)[-period:]
        l = list(losses)[-period:]
        avg_gain = sum(g) / period
        avg_loss = sum(l) / period
        if avg_loss == 0.0:
            return 100.0
        rs = avg_gain / avg_loss
        return 100.0 - (100.0 / (1.0 + rs))

    # ─── MACD ───────────────────────────────────────────────────
    @staticmethod
    def macd(
        state: SymbolState, close: float
    ) -> tuple[float | None, float | None, float | None]:
        """Returns (macd_value, signal_line, histogram). Updates state.ema_states in-place."""
        ema12 = TechnicalIndicators.ema(close, state.ema_states.get("EMA12"), 12)
        ema26 = TechnicalIndicators.ema(close, state.ema_states.get("EMA26"), 26)
        state.ema_states["EMA12"] = ema12
        state.ema_states["EMA26"] = ema26

        # Need at least 1 EMA26 seed before emitting MACD
        if len(state.closes) < 26:
            return None, None, None

        macd_val = ema12 - ema26
        state.macd_history.append(macd_val)

        signal = TechnicalIndicators.ema(macd_val, state.ema_states.get("MACD_signal"), 9)
        state.ema_states["MACD_signal"] = signal

        histogram = macd_val - signal if signal is not None else None
        return macd_val, signal, histogram

    # ─── Bollinger Bands (20, 2σ) ───────────────────────────────
    @staticmethod
    def bollinger_bands(
        prices: deque, period: int = 20, std_mult: float = 2.0
    ) -> tuple[float | None, float | None, float | None]:
        """Returns (middle, upper, lower)."""
        if len(prices) < period:
            return None, None, None
        recent = list(prices)[-period:]
        mean = sum(recent) / period
        variance = sum((p - mean) ** 2 for p in recent) / period
        std = math.sqrt(variance)
        return mean, mean + std_mult * std, mean - std_mult * std

    # ─── ATR (14, Wilder-smoothed) ──────────────────────────────
    @staticmethod
    def atr(
        highs: deque,
        lows: deque,
        closes: deque,
        period: int = 14,
    ) -> float | None:
        if len(closes) < period + 1:
            return None
        h_list = list(highs)[-(period + 1) :]
        l_list = list(lows)[-(period + 1) :]
        c_list = list(closes)[-(period + 1) :]

        true_ranges = []
        for i in range(1, len(h_list)):
            prev_close = c_list[i - 1]
            tr = max(
                h_list[i] - l_list[i],
                abs(h_list[i] - prev_close),
                abs(l_list[i] - prev_close),
            )
            true_ranges.append(tr)

        if not true_ranges:
            return None
        return sum(true_ranges) / len(true_ranges)

    # ─── VWAP (daily reset at 09:30 ET) ─────────────────────────
    @staticmethod
    def vwap(state: SymbolState, date_str: str, high: float, low: float, close: float, volume: int) -> float:
        """Cumulative intraday VWAP. Resets each day."""
        typical_price = (high + low + close) / 3.0
        if state.daily_vwap_date != date_str:
            state.daily_vwap_date = date_str
            state.daily_pv_sum = 0.0
            state.daily_vol_sum = 0.0
        state.daily_pv_sum += typical_price * volume
        state.daily_vol_sum += volume
        if state.daily_vol_sum == 0:
            return typical_price
        return state.daily_pv_sum / state.daily_vol_sum

    # ─── Compute all indicators and return records ───────────────
    def compute_all(
        self,
        state: SymbolState,
        symbol: str,
        timeframe: str,
        bar: dict,
    ) -> list[dict]:
        """Compute all indicators from state and return a list of technical_indicator dicts."""
        close = bar["close"]
        high = bar["high"]
        low = bar["low"]
        volume = int(bar["volume"])
        ts_ms = bar["timestamp_ms"]
        # date string for VWAP reset (UTC)
        from datetime import datetime, timezone
        date_str = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d")

        results = []

        def _make(name: str, value: float | None, **kwargs) -> dict | None:
            if value is None:
                return None
            return {
                "indicator_id": str(uuid.uuid4()),
                "symbol": symbol,
                "timestamp_ms": ts_ms,
                "indicator_name": name,
                "timeframe": timeframe,
                "value": value,
                "signal_value": kwargs.get("signal_value"),
                "upper_band": kwargs.get("upper_band"),
                "lower_band": kwargs.get("lower_band"),
                "metadata": {k: str(v) for k, v in kwargs.get("metadata", {}).items()},
            }

        # SMA
        for n in [10, 20, 50, 200]:
            rec = _make(f"SMA{n}", self.sma(state.closes, n))
            if rec:
                results.append(rec)

        # EMA (incremental — updates ema_states)
        for n, key in [(12, "EMA12"), (26, "EMA26")]:
            ema_val = self.ema(close, state.ema_states.get(key), n)
            state.ema_states[key] = ema_val
            rec = _make(f"EMA{n}", ema_val)
            if rec:
                results.append(rec)

        # RSI
        rsi_val = self.rsi(state.rsi_gains, state.rsi_losses)
        rec = _make("RSI14", rsi_val)
        if rec:
            results.append(rec)

        # MACD
        macd_val, signal_line, histogram = self.macd(state, close)
        if macd_val is not None:
            rec = _make("MACD", macd_val, signal_value=signal_line, metadata={"histogram": histogram})
            if rec:
                results.append(rec)

        # Bollinger Bands
        bb_mid, bb_upper, bb_lower = self.bollinger_bands(state.closes)
        if bb_mid is not None:
            rec = _make("BB20", bb_mid, upper_band=bb_upper, lower_band=bb_lower)
            if rec:
                results.append(rec)

        # ATR
        atr_val = self.atr(state.highs, state.lows, state.closes)
        rec = _make("ATR14", atr_val)
        if rec:
            results.append(rec)

        # VWAP
        vwap_val = self.vwap(state, date_str, high, low, close, volume)
        rec = _make("VWAP", vwap_val)
        if rec:
            results.append(rec)

        return results
