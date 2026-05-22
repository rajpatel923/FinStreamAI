"""Tests for DataCleaner and DataCleaningJob."""
import time
import uuid
from unittest.mock import MagicMock, patch

import pytest

from src.processors.data_cleaner import DataCleaner
from src.state.rolling_state import RollingState


@pytest.fixture
def rolling_state():
    return RollingState()


@pytest.fixture
def cleaner(rolling_state):
    return DataCleaner(rolling_state)


@pytest.fixture
def valid_tick():
    return {
        "event_id": str(uuid.uuid4()),
        "symbol": "AAPL",
        "timestamp_ms": int(time.time() * 1000),
        "price": 182.50,
        "volume": 1000,
        "bid_price": None,
        "ask_price": None,
        "bid_size": None,
        "ask_size": None,
        "exchange": None,
        "source": "polygon",
        "is_mock": False,
    }


# ─── Validation tests ────────────────────────────────────────────────────────────

class TestValidation:
    def test_valid_tick_passes(self, cleaner, valid_tick):
        ok, reason = cleaner.validate(valid_tick)
        assert ok is True
        assert reason == ""

    def test_empty_symbol_rejected(self, cleaner, valid_tick):
        valid_tick["symbol"] = ""
        ok, reason = cleaner.validate(valid_tick)
        assert ok is False
        assert reason == "empty_symbol"

    def test_zero_price_rejected(self, cleaner, valid_tick):
        valid_tick["price"] = 0.0
        ok, reason = cleaner.validate(valid_tick)
        assert ok is False
        assert reason == "invalid_price"

    def test_negative_price_rejected(self, cleaner, valid_tick):
        valid_tick["price"] = -10.0
        ok, reason = cleaner.validate(valid_tick)
        assert ok is False
        assert reason == "invalid_price"

    def test_negative_volume_rejected(self, cleaner, valid_tick):
        valid_tick["volume"] = -1
        ok, reason = cleaner.validate(valid_tick)
        assert ok is False
        assert reason == "invalid_volume"

    def test_zero_volume_accepted(self, cleaner, valid_tick):
        valid_tick["volume"] = 0
        ok, reason = cleaner.validate(valid_tick)
        assert ok is True

    def test_stale_timestamp_rejected(self, cleaner, valid_tick):
        # Timestamp 10 seconds old → stale
        valid_tick["timestamp_ms"] = int(time.time() * 1000) - 10_000
        ok, reason = cleaner.validate(valid_tick)
        assert ok is False
        assert reason == "stale_timestamp"


# ─── Deduplication tests ─────────────────────────────────────────────────────────

class TestDeduplication:
    def test_first_tick_not_duplicate(self, cleaner, valid_tick):
        assert cleaner.is_duplicate(valid_tick) is False

    def test_same_tick_is_duplicate(self, cleaner, valid_tick):
        cleaner.is_duplicate(valid_tick)  # register
        assert cleaner.is_duplicate(valid_tick) is True

    def test_different_price_not_duplicate(self, cleaner, valid_tick):
        cleaner.is_duplicate(valid_tick)
        tick2 = dict(valid_tick)
        tick2["price"] = 183.00
        assert cleaner.is_duplicate(tick2) is False

    def test_different_timestamp_not_duplicate(self, cleaner, valid_tick):
        cleaner.is_duplicate(valid_tick)
        tick2 = dict(valid_tick)
        tick2["timestamp_ms"] = valid_tick["timestamp_ms"] + 1000
        assert cleaner.is_duplicate(tick2) is False


# ─── Outlier tagging ─────────────────────────────────────────────────────────────

class TestOutlierTagging:
    def test_normal_tick_not_outlier(self, cleaner, rolling_state, valid_tick):
        # Seed state with normal prices
        for price in [182.0, 182.5, 183.0, 182.8, 182.2] * 8:
            rolling_state.update_tick("AAPL", price, 1000, int(time.time() * 1000))
        result = cleaner.tag_outlier(valid_tick)
        assert "_is_outlier" in result
        assert "_zscore" in result

    def test_extreme_price_tagged_as_outlier(self, cleaner, rolling_state, valid_tick):
        # Seed with small variance (std > 0 required for Z-score)
        for price in [100.0, 100.1, 99.9, 100.2, 99.8] * 8:
            rolling_state.update_tick("AAPL", price, 1000, int(time.time() * 1000))
        valid_tick["price"] = 500.0  # extreme outlier (>> 3σ)
        result = cleaner.tag_outlier(valid_tick)
        assert result["_is_outlier"] is True

    def test_outlier_not_dropped(self, cleaner, rolling_state, valid_tick):
        """Outlier ticks should be tagged but still returned."""
        for price in [100.0] * 40:
            rolling_state.update_tick("AAPL", price, 1000, int(time.time() * 1000))
        valid_tick["price"] = 500.0
        result = cleaner.tag_outlier(valid_tick)
        assert result is not None
        assert result["price"] == 500.0


# ─── Full clean pipeline ─────────────────────────────────────────────────────────

class TestCleanPipeline:
    def test_valid_tick_passes_through(self, cleaner, valid_tick):
        result = cleaner.clean(valid_tick)
        assert result is not None
        assert result["symbol"] == "AAPL"

    def test_invalid_tick_returns_none(self, cleaner, valid_tick):
        valid_tick["price"] = -1.0
        assert cleaner.clean(valid_tick) is None

    def test_duplicate_returns_none(self, cleaner, valid_tick):
        cleaner.clean(valid_tick)  # first pass
        second = dict(valid_tick)
        assert cleaner.clean(second) is None  # duplicate

    def test_clean_updates_rolling_state(self, cleaner, rolling_state, valid_tick):
        cleaner.clean(valid_tick)
        state = rolling_state.get_or_create("AAPL")
        assert len(state.prices) == 1
        assert state.prices[0] == 182.50


# ─── DataCleaningJob integration ────────────────────────────────────────────────

class TestDataCleaningJob:
    def test_job_skips_invalid_record(self):
        from src.jobs.data_cleaning_job import DataCleaningJob

        job = DataCleaningJob()
        bad_tick = {
            "event_id": "x", "symbol": "", "timestamp_ms": int(time.time() * 1000),
            "price": 100.0, "volume": 100, "bid_price": None, "ask_price": None,
            "bid_size": None, "ask_size": None, "exchange": None,
            "source": "test", "is_mock": False,
        }
        outputs = job.process(bad_tick, MagicMock())
        assert outputs == []

    def test_job_produces_clean_tick(self):
        from src.jobs.data_cleaning_job import DataCleaningJob

        job = DataCleaningJob()
        tick = {
            "event_id": str(uuid.uuid4()), "symbol": "AAPL",
            "timestamp_ms": int(time.time() * 1000),
            "price": 182.50, "volume": 1000, "bid_price": None, "ask_price": None,
            "bid_size": None, "ask_size": None, "exchange": None,
            "source": "polygon", "is_mock": False,
        }
        outputs = job.process(tick, MagicMock())
        assert len(outputs) == 1
        topic, key_bytes, value_bytes = outputs[0]
        assert topic == "market.ticks.clean"
        assert key_bytes == b"AAPL"
        assert len(value_bytes) > 5  # Avro with wire format header
