"""Tests for incremental technical indicator computations."""
import math
from collections import deque

import pytest

from src.processors.technical_indicators import TechnicalIndicators
from src.state.rolling_state import SymbolState


@pytest.fixture
def ti():
    return TechnicalIndicators()


@pytest.fixture
def state():
    return SymbolState()


# ─── SMA ────────────────────────────────────────────────────────────────────────

class TestSMA:
    def test_sma_exact(self):
        prices = deque([10.0, 20.0, 30.0, 40.0, 50.0], maxlen=200)
        result = TechnicalIndicators.sma(prices, 5)
        assert result == pytest.approx(30.0)

    def test_sma_insufficient_data(self):
        prices = deque([10.0, 20.0], maxlen=200)
        assert TechnicalIndicators.sma(prices, 5) is None

    def test_sma_uses_last_n(self):
        prices = deque([1.0, 2.0, 100.0, 200.0, 300.0], maxlen=200)
        # SMA(3) should use last 3 elements: 100, 200, 300
        result = TechnicalIndicators.sma(prices, 3)
        assert result == pytest.approx(200.0)

    def test_sma_single_element(self):
        prices = deque([42.0], maxlen=200)
        assert TechnicalIndicators.sma(prices, 1) == pytest.approx(42.0)


# ─── EMA ────────────────────────────────────────────────────────────────────────

class TestEMA:
    def test_ema_no_prev_returns_price(self):
        result = TechnicalIndicators.ema(100.0, None, 12)
        assert result == pytest.approx(100.0)

    def test_ema_formula(self):
        # k = 2/(12+1) = 2/13 ≈ 0.1538
        k = 2.0 / 13
        price = 110.0
        prev_ema = 100.0
        expected = price * k + prev_ema * (1 - k)
        result = TechnicalIndicators.ema(price, prev_ema, 12)
        assert result == pytest.approx(expected)

    def test_ema_converges(self):
        """After many periods of constant price, EMA should converge to that price."""
        ema = None
        price = 50.0
        for _ in range(100):
            ema = TechnicalIndicators.ema(price, ema, 12)
        assert ema == pytest.approx(price, rel=1e-3)

    def test_ema_faster_for_smaller_period(self):
        """EMA(12) reacts faster than EMA(26) to price changes."""
        ema12 = 100.0
        ema26 = 100.0
        new_price = 200.0
        new_ema12 = TechnicalIndicators.ema(new_price, ema12, 12)
        new_ema26 = TechnicalIndicators.ema(new_price, ema26, 26)
        assert new_ema12 > new_ema26  # EMA(12) reacts more


# ─── RSI ────────────────────────────────────────────────────────────────────────

class TestRSI:
    def test_rsi_insufficient_data(self):
        gains = deque([1.0] * 10, maxlen=50)
        losses = deque([0.5] * 10, maxlen=50)
        assert TechnicalIndicators.rsi(gains, losses, 14) is None

    def test_rsi_all_gains_returns_100(self):
        gains = deque([1.0] * 14, maxlen=50)
        losses = deque([0.0] * 14, maxlen=50)
        result = TechnicalIndicators.rsi(gains, losses, 14)
        assert result == pytest.approx(100.0)

    def test_rsi_all_losses_returns_0(self):
        gains = deque([0.0] * 14, maxlen=50)
        losses = deque([1.0] * 14, maxlen=50)
        result = TechnicalIndicators.rsi(gains, losses, 14)
        assert result == pytest.approx(0.0)

    def test_rsi_balanced(self):
        """Equal gains and losses → RSI = 50."""
        gains = deque([1.0] * 14, maxlen=50)
        losses = deque([1.0] * 14, maxlen=50)
        result = TechnicalIndicators.rsi(gains, losses, 14)
        assert result == pytest.approx(50.0)

    def test_rsi_range(self):
        gains = deque([2.0, 1.5, 0.5, 3.0, 1.0, 0.0, 2.5, 1.0, 0.5, 1.5, 2.0, 1.0, 0.5, 1.0], maxlen=50)
        losses = deque([0.5, 0.0, 1.0, 0.5, 2.0, 1.5, 0.0, 0.5, 1.0, 0.5, 0.0, 0.5, 1.5, 0.5], maxlen=50)
        result = TechnicalIndicators.rsi(gains, losses)
        assert 0.0 <= result <= 100.0


# ─── MACD ───────────────────────────────────────────────────────────────────────

class TestMACD:
    def test_macd_insufficient_data(self, state):
        """MACD returns (None, None, None) before 26 bars."""
        macd_val, signal, hist = TechnicalIndicators.macd(state, 100.0)
        assert macd_val is None
        assert signal is None
        assert hist is None

    def test_macd_emits_after_warmup(self, state):
        """MACD should emit after 26 bars."""
        for i in range(26):
            state.closes.append(100.0 + i)
        macd_val, signal, hist = TechnicalIndicators.macd(state, 126.0)
        assert macd_val is not None

    def test_macd_ema_states_updated(self, state):
        for _ in range(30):
            state.closes.append(100.0)
        TechnicalIndicators.macd(state, 100.0)
        assert "EMA12" in state.ema_states
        assert "EMA26" in state.ema_states
        assert "MACD_signal" in state.ema_states


# ─── Bollinger Bands ─────────────────────────────────────────────────────────────

class TestBollingerBands:
    def test_bb_insufficient_data(self):
        prices = deque([100.0] * 10, maxlen=200)
        mid, upper, lower = TechnicalIndicators.bollinger_bands(prices, 20)
        assert mid is None and upper is None and lower is None

    def test_bb_constant_prices(self):
        """With constant prices, std=0 → upper=lower=mid."""
        prices = deque([100.0] * 20, maxlen=200)
        mid, upper, lower = TechnicalIndicators.bollinger_bands(prices, 20)
        assert mid == pytest.approx(100.0)
        assert upper == pytest.approx(100.0)
        assert lower == pytest.approx(100.0)

    def test_bb_upper_gt_lower(self):
        prices = deque([100.0, 102.0, 98.0, 105.0, 97.0] * 5, maxlen=200)
        mid, upper, lower = TechnicalIndicators.bollinger_bands(prices, 20)
        assert upper > mid > lower

    def test_bb_symmetry(self):
        prices = deque([100.0, 102.0, 98.0, 105.0, 97.0] * 5, maxlen=200)
        mid, upper, lower = TechnicalIndicators.bollinger_bands(prices, 20)
        assert upper - mid == pytest.approx(mid - lower, rel=1e-6)


# ─── ATR ────────────────────────────────────────────────────────────────────────

class TestATR:
    def test_atr_insufficient_data(self):
        highs = deque([105.0] * 5, maxlen=200)
        lows = deque([95.0] * 5, maxlen=200)
        closes = deque([100.0] * 5, maxlen=200)
        assert TechnicalIndicators.atr(highs, lows, closes) is None

    def test_atr_basic(self):
        # Simple case: high-low spread = 10, no gaps → ATR ≈ 10
        highs = deque([110.0] * 20, maxlen=200)
        lows = deque([100.0] * 20, maxlen=200)
        closes = deque([105.0] * 20, maxlen=200)
        result = TechnicalIndicators.atr(highs, lows, closes, period=14)
        assert result == pytest.approx(10.0)

    def test_atr_positive(self):
        highs = deque([100.0 + i for i in range(20)], maxlen=200)
        lows = deque([90.0 + i for i in range(20)], maxlen=200)
        closes = deque([95.0 + i for i in range(20)], maxlen=200)
        result = TechnicalIndicators.atr(highs, lows, closes)
        assert result is not None and result > 0


# ─── VWAP ───────────────────────────────────────────────────────────────────────

class TestVWAP:
    def test_vwap_single_bar(self, state):
        result = TechnicalIndicators.vwap(state, "2025-05-07", 110.0, 90.0, 100.0, 1000)
        # typical price = (110 + 90 + 100) / 3 = 100
        assert result == pytest.approx(100.0)

    def test_vwap_resets_daily(self, state):
        TechnicalIndicators.vwap(state, "2025-05-07", 110.0, 90.0, 100.0, 1000)
        assert state.daily_vwap_date == "2025-05-07"
        # New day
        TechnicalIndicators.vwap(state, "2025-05-08", 200.0, 180.0, 190.0, 500)
        assert state.daily_vwap_date == "2025-05-08"
        # PV sum should only reflect new day
        expected_tp = (200.0 + 180.0 + 190.0) / 3.0
        assert state.daily_pv_sum == pytest.approx(expected_tp * 500)

    def test_vwap_cumulative(self, state):
        vwap1 = TechnicalIndicators.vwap(state, "2025-05-07", 110.0, 90.0, 100.0, 1000)
        vwap2 = TechnicalIndicators.vwap(state, "2025-05-07", 120.0, 100.0, 110.0, 2000)
        # tp1 = 100, tp2 = (120+100+110)/3 ≈ 110
        tp1 = (110 + 90 + 100) / 3.0
        tp2 = (120 + 100 + 110) / 3.0
        expected_vwap2 = (tp1 * 1000 + tp2 * 2000) / 3000
        assert vwap2 == pytest.approx(expected_vwap2)


# ─── compute_all integration ─────────────────────────────────────────────────────

class TestComputeAll:
    def test_compute_all_warmup(self, ti, state):
        """compute_all returns empty list when not enough data."""
        bar = {
            "open": 100.0, "high": 102.0, "low": 99.0, "close": 101.0,
            "volume": 1000, "timestamp_ms": 1000000,
        }
        result = ti.compute_all(state, "AAPL", "1min", bar)
        # With empty state, only a few indicators will emit (EMA, VWAP always emit)
        # At minimum VWAP should be present
        names = {r["indicator_name"] for r in result}
        assert "VWAP" in names

    def test_compute_all_with_data(self, ti, state):
        """After enough bars, all indicators should emit."""
        for i in range(30):
            state.closes.append(100.0 + i)
            state.highs.append(102.0 + i)
            state.lows.append(98.0 + i)
            state.rsi_gains.append(1.0)
            state.rsi_losses.append(0.5)
            state.prev_close = 100.0 + i - 1 if i > 0 else None

        bar = {
            "open": 129.0, "high": 131.0, "low": 128.0, "close": 130.0,
            "volume": 5000, "timestamp_ms": 1_700_000_000_000,
        }
        result = ti.compute_all(state, "AAPL", "1min", bar)
        names = {r["indicator_name"] for r in result}
        assert "SMA10" in names
        assert "EMA12" in names
        assert "RSI14" in names
        assert "BB20" in names
        assert "ATR14" in names
        assert "VWAP" in names

    def test_indicator_record_schema(self, ti, state):
        """Each result record must have required fields."""
        bar = {
            "open": 100.0, "high": 102.0, "low": 99.0, "close": 101.0,
            "volume": 1000, "timestamp_ms": 1_700_000_000_000,
        }
        result = ti.compute_all(state, "AAPL", "1min", bar)
        for rec in result:
            assert "indicator_id" in rec
            assert "symbol" in rec
            assert "indicator_name" in rec
            assert "value" in rec
            assert rec["symbol"] == "AAPL"
