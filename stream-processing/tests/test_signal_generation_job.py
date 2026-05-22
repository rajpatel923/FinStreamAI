"""Tests for SignalGenerationJob."""
import time
import uuid
from unittest.mock import MagicMock

import pytest

from src.config import settings


def make_indicator(symbol="AAPL", name="RSI14", value=45.0, signal_val=None, upper=None, lower=None):
    return {
        "indicator_id": str(uuid.uuid4()),
        "symbol": symbol,
        "timestamp_ms": int(time.time() * 1000),
        "indicator_name": name,
        "timeframe": "1min",
        "value": value,
        "signal_value": signal_val,
        "upper_band": upper,
        "lower_band": lower,
        "metadata": {},
    }


class TestSignalGenerationJob:
    def test_rsi_oversold_generates_buy(self, mock_timescaledb_sink):
        from src.jobs.signal_generation_job import SignalGenerationJob

        job = SignalGenerationJob(db_sink=mock_timescaledb_sink)
        rsi_oversold = settings.RSI_OVERSOLD - 5.0  # clearly oversold
        ind = make_indicator(name="RSI14", value=rsi_oversold)
        outputs = job.process(ind, MagicMock())
        assert len(outputs) >= 1
        import json
        sig = json.loads(outputs[0][2])
        assert sig["direction"] == "BUY"
        assert sig["signal_type"] == "RSI"
        assert 0 < sig["strength"] <= 1.0

    def test_rsi_overbought_generates_sell(self, mock_timescaledb_sink):
        from src.jobs.signal_generation_job import SignalGenerationJob

        job = SignalGenerationJob(db_sink=mock_timescaledb_sink)
        rsi_overbought = settings.RSI_OVERBOUGHT + 5.0
        ind = make_indicator(name="RSI14", value=rsi_overbought)
        outputs = job.process(ind, MagicMock())
        assert len(outputs) >= 1
        import json
        sig = json.loads(outputs[0][2])
        assert sig["direction"] == "SELL"
        assert sig["signal_type"] == "RSI"

    def test_rsi_neutral_no_signal(self, mock_timescaledb_sink):
        from src.jobs.signal_generation_job import SignalGenerationJob

        job = SignalGenerationJob(db_sink=mock_timescaledb_sink)
        # RSI in neutral zone
        ind = make_indicator(name="RSI14", value=50.0)
        outputs = job.process(ind, MagicMock())
        rsi_signals = [o for o in outputs if b'"RSI"' in o[2]]
        assert rsi_signals == []

    def test_signal_deduplication(self, mock_timescaledb_sink):
        from src.jobs.signal_generation_job import SignalGenerationJob

        job = SignalGenerationJob(db_sink=mock_timescaledb_sink)
        rsi_oversold = settings.RSI_OVERSOLD - 5.0
        ts_now = int(time.time() * 1000)
        ind1 = make_indicator(name="RSI14", value=rsi_oversold)
        ind1["timestamp_ms"] = ts_now

        # First signal should be emitted
        outputs1 = job.process(ind1, MagicMock())
        assert len(outputs1) >= 1

        # Second signal within dedup window should be suppressed
        ind2 = make_indicator(name="RSI14", value=rsi_oversold)
        ind2["timestamp_ms"] = ts_now + 1000  # 1 second later, within 5-min window
        outputs2 = job.process(ind2, MagicMock())
        rsi_signals2 = [o for o in outputs2 if b'"RSI"' in o[2]]
        assert rsi_signals2 == []

    def test_signal_after_dedup_window_emitted(self, mock_timescaledb_sink):
        from src.jobs.signal_generation_job import SignalGenerationJob

        job = SignalGenerationJob(db_sink=mock_timescaledb_sink)
        rsi_oversold = settings.RSI_OVERSOLD - 5.0
        ts_now = int(time.time() * 1000)

        ind1 = make_indicator(name="RSI14", value=rsi_oversold)
        ind1["timestamp_ms"] = ts_now
        job.process(ind1, MagicMock())

        # Signal past dedup window should be emitted again
        dedup_window_ms = settings.SIGNAL_DEDUP_WINDOW_S * 1000
        ind2 = make_indicator(name="RSI14", value=rsi_oversold)
        ind2["timestamp_ms"] = ts_now + dedup_window_ms + 1000
        outputs = job.process(ind2, MagicMock())
        rsi_signals = [o for o in outputs if b'"RSI"' in o[2]]
        assert len(rsi_signals) >= 1

    def test_macd_bullish_crossover_buy(self, mock_timescaledb_sink):
        from src.jobs.signal_generation_job import SignalGenerationJob

        job = SignalGenerationJob(db_sink=mock_timescaledb_sink)
        ts_now = int(time.time() * 1000)

        # Simulate negative histogram then positive (bullish crossover)
        ind_neg = make_indicator(name="MACD", value=-0.5, signal_val=0.2)
        ind_neg["timestamp_ms"] = ts_now - 60_000
        job.process(ind_neg, MagicMock())  # set negative histogram

        ind_pos = make_indicator(name="MACD", value=0.5, signal_val=0.1)
        ind_pos["timestamp_ms"] = ts_now
        outputs = job.process(ind_pos, MagicMock())

        import json
        macd_signals = [json.loads(o[2]) for o in outputs if b'"MACD_CROSSOVER"' in o[2]]
        assert len(macd_signals) >= 1
        assert macd_signals[0]["direction"] == "BUY"

    def test_macd_bearish_crossover_sell(self, mock_timescaledb_sink):
        from src.jobs.signal_generation_job import SignalGenerationJob

        job = SignalGenerationJob(db_sink=mock_timescaledb_sink)
        ts_now = int(time.time() * 1000)

        ind_pos = make_indicator(name="MACD", value=0.5, signal_val=0.1)
        ind_pos["timestamp_ms"] = ts_now - 60_000
        job.process(ind_pos, MagicMock())

        ind_neg = make_indicator(name="MACD", value=-0.5, signal_val=0.2)
        ind_neg["timestamp_ms"] = ts_now
        outputs = job.process(ind_neg, MagicMock())

        import json
        macd_signals = [json.loads(o[2]) for o in outputs if b'"MACD_CROSSOVER"' in o[2]]
        assert len(macd_signals) >= 1
        assert macd_signals[0]["direction"] == "SELL"

    def test_signal_strength_capped_at_1(self, mock_timescaledb_sink):
        from src.jobs.signal_generation_job import SignalGenerationJob

        job = SignalGenerationJob(db_sink=mock_timescaledb_sink)
        # RSI=0 → maximum strength
        ind = make_indicator(name="RSI14", value=0.0)
        outputs = job.process(ind, MagicMock())
        import json
        for _, _, val in outputs:
            sig = json.loads(val)
            assert sig["strength"] <= 1.0

    def test_db_write_called_on_signal(self, mock_timescaledb_sink):
        from src.jobs.signal_generation_job import SignalGenerationJob

        job = SignalGenerationJob(db_sink=mock_timescaledb_sink)
        ind = make_indicator(name="RSI14", value=settings.RSI_OVERSOLD - 5.0)
        job.process(ind, MagicMock())
        mock_timescaledb_sink.write_trading_signal.assert_called()

    def test_signal_record_has_required_fields(self, mock_timescaledb_sink):
        from src.jobs.signal_generation_job import SignalGenerationJob

        job = SignalGenerationJob(db_sink=mock_timescaledb_sink)
        ind = make_indicator(name="RSI14", value=settings.RSI_OVERSOLD - 5.0)
        outputs = job.process(ind, MagicMock())
        import json
        for _, _, val in outputs:
            sig = json.loads(val)
            assert "signal_id" in sig
            assert "symbol" in sig
            assert "timestamp_ms" in sig
            assert "signal_type" in sig
            assert "direction" in sig
            assert "strength" in sig
