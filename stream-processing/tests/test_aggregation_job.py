"""Tests for WindowState and AggregationJob."""
import time
import uuid
from unittest.mock import MagicMock

import pytest

from src.state.window_state import TIMEFRAME_MS, WindowBuffer, WindowState


@pytest.fixture
def window_state():
    return WindowState()


@pytest.fixture
def base_tick():
    return {
        "event_id": str(uuid.uuid4()),
        "symbol": "AAPL",
        "timestamp_ms": 1_700_000_000_000,  # fixed epoch for predictable windows
        "price": 182.50,
        "volume": 1000,
        "source": "polygon",
        "is_mock": False,
    }


# ─── Window assignment ───────────────────────────────────────────────────────────

class TestWindowAssignment:
    def test_5min_window_alignment(self):
        ts_ms = 1_700_000_000_000  # some epoch
        start, end = WindowState.assign_window(ts_ms, "5min")
        tf_ms = TIMEFRAME_MS["5min"]
        assert start % tf_ms == 0
        assert end == start + tf_ms
        assert start <= ts_ms < end

    def test_15min_window_alignment(self):
        ts_ms = 1_700_000_000_000
        start, end = WindowState.assign_window(ts_ms, "15min")
        tf_ms = TIMEFRAME_MS["15min"]
        assert start % tf_ms == 0
        assert end - start == tf_ms

    def test_1hour_window_alignment(self):
        ts_ms = 1_700_000_000_000
        start, end = WindowState.assign_window(ts_ms, "1hour")
        tf_ms = TIMEFRAME_MS["1hour"]
        assert start % tf_ms == 0
        assert end - start == tf_ms

    def test_same_5min_window_for_nearby_ticks(self):
        ts1 = 1_700_000_000_000
        ts2 = ts1 + 60_000  # 1 minute later, same 5-min window
        start1, _ = WindowState.assign_window(ts1, "5min")
        start2, _ = WindowState.assign_window(ts2, "5min")
        assert start1 == start2

    def test_different_5min_windows(self):
        ts1 = 1_700_000_000_000
        ts2 = ts1 + TIMEFRAME_MS["5min"]  # exactly one window later
        start1, _ = WindowState.assign_window(ts1, "5min")
        start2, _ = WindowState.assign_window(ts2, "5min")
        assert start2 == start1 + TIMEFRAME_MS["5min"]


# ─── WindowState update ──────────────────────────────────────────────────────────

class TestWindowStateUpdate:
    def test_update_creates_buffers_for_all_timeframes(self, window_state, base_tick):
        window_state.update("AAPL", base_tick)
        assert window_state.active_count() == len(TIMEFRAME_MS)

    def test_ohlcv_computation(self, window_state):
        ts = 1_700_000_000_000
        ticks = [
            {"symbol": "AAPL", "timestamp_ms": ts + i * 10_000, "price": p, "volume": v}
            for i, (p, v) in enumerate([(100.0, 500), (105.0, 300), (98.0, 200), (102.0, 100)])
        ]
        for tick in ticks:
            window_state.update("AAPL", tick)

        # Find the 5-min buffer
        start, _ = WindowState.assign_window(ts, "5min")
        key = ("AAPL", "5min", start)
        buf = window_state._buffers[key]

        assert buf.open_price == pytest.approx(100.0)
        assert buf.high_price == pytest.approx(105.0)
        assert buf.low_price == pytest.approx(98.0)
        assert buf.close_price == pytest.approx(102.0)
        assert buf.volume_sum == 1100
        assert buf.trade_count == 4

    def test_vwap_in_build_bar_record(self, window_state):
        ts = 1_700_000_000_000
        ticks = [
            {"symbol": "AAPL", "timestamp_ms": ts, "price": 100.0, "volume": 1000},
            {"symbol": "AAPL", "timestamp_ms": ts + 10_000, "price": 200.0, "volume": 1000},
        ]
        for tick in ticks:
            window_state.update("AAPL", tick)

        start, _ = WindowState.assign_window(ts, "5min")
        buf = window_state._buffers[("AAPL", "5min", start)]
        bar = WindowState.build_bar_record(buf)
        expected_vwap = (100.0 * 1000 + 200.0 * 1000) / 2000
        assert bar["vwap"] == pytest.approx(expected_vwap)


# ─── Closeable windows ───────────────────────────────────────────────────────────

class TestCloseableWindows:
    def test_no_closeable_windows_before_grace(self, window_state, base_tick):
        window_state.update("AAPL", base_tick)
        # now_ms is before window_end + grace
        closeable = window_state.get_closeable_windows(base_tick["timestamp_ms"] + 1000)
        assert closeable == []

    def test_closeable_after_grace_period(self, window_state, base_tick):
        from src.config import settings

        window_state.update("AAPL", base_tick)
        ts = base_tick["timestamp_ms"]
        # Move past window end + grace for 5min window
        future_ms = ts + TIMEFRAME_MS["5min"] + settings.WINDOW_GRACE_PERIOD_MS + 1000
        closeable = window_state.get_closeable_windows(future_ms)
        timeframes = {c.timeframe for c in closeable}
        assert "5min" in timeframes

    def test_remove_window(self, window_state, base_tick):
        window_state.update("AAPL", base_tick)
        initial_count = window_state.active_count()
        start, _ = WindowState.assign_window(base_tick["timestamp_ms"], "5min")
        window_state.remove_window("AAPL", "5min", start)
        assert window_state.active_count() == initial_count - 1


# ─── build_bar_record ────────────────────────────────────────────────────────────

class TestBuildBarRecord:
    def test_bar_record_schema(self):
        buf = WindowBuffer(
            symbol="AAPL", timeframe="5min",
            window_start_ms=1_700_000_000_000,
            window_end_ms=1_700_000_300_000,
            open_price=100.0, high_price=105.0, low_price=98.0,
            close_price=103.0, close_timestamp_ms=1_700_000_290_000,
            volume_sum=5000, pv_sum=515_000.0, trade_count=50,
        )
        bar = WindowState.build_bar_record(buf)
        assert bar["symbol"] == "AAPL"
        assert bar["timeframe"] == "5min"
        assert bar["open"] == 100.0
        assert bar["high"] == 105.0
        assert bar["low"] == 98.0
        assert bar["close"] == 103.0
        assert bar["volume"] == 5000
        assert bar["trade_count"] == 50
        assert "bar_id" in bar
        assert bar["vwap"] == pytest.approx(515_000.0 / 5000)


# ─── AggregationJob integration ─────────────────────────────────────────────────

class TestAggregationJob:
    def test_process_returns_empty_list(self, mock_timescaledb_sink):
        from src.jobs.aggregation_job import AggregationJob

        job = AggregationJob(db_sink=mock_timescaledb_sink)
        tick = {
            "event_id": "x", "symbol": "AAPL",
            "timestamp_ms": int(time.time() * 1000),
            "price": 182.0, "volume": 1000,
            "bid_price": None, "ask_price": None, "bid_size": None, "ask_size": None,
            "exchange": None, "source": "polygon", "is_mock": False,
        }
        outputs = job.process(tick, MagicMock())
        assert outputs == []  # bars emitted in _flush_pending, not process()

    def test_flush_pending_emits_closed_bars(self, mock_timescaledb_sink):
        from src.config import settings
        from src.jobs.aggregation_job import AggregationJob, _TOPIC_MAP

        job = AggregationJob(db_sink=mock_timescaledb_sink)
        ts = 1_700_000_000_000
        tick = {
            "event_id": "x", "symbol": "AAPL",
            "timestamp_ms": ts,
            "price": 182.0, "volume": 1000,
            "bid_price": None, "ask_price": None, "bid_size": None, "ask_size": None,
            "exchange": None, "source": "polygon", "is_mock": False,
        }
        job._aggregator.ingest(tick)

        # Move time well past all window ends + grace
        future_ms = ts + TIMEFRAME_MS["1hour"] + settings.WINDOW_GRACE_PERIOD_MS + 60_000

        # Capture produce calls
        produced = []
        job._sink.produce = lambda topic, key, value: produced.append((topic, key, value))

        job._flush_pending(future_ms)
        assert len(produced) == len(TIMEFRAME_MS)  # one bar per timeframe
        topics = {p[0] for p in produced}
        assert "market.bars.5min" in topics
        assert "market.bars.15min" in topics
        assert "market.bars.1hour" in topics
        assert mock_timescaledb_sink.write_market_bar.call_count == len(TIMEFRAME_MS)
