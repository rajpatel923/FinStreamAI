"""Tests for FeatureEngineeringJob."""
import time
import uuid
from unittest.mock import MagicMock

import pytest

from src.config import settings


def make_bar(symbol="AAPL", ts_offset_s=0):
    return {
        "bar_id": str(uuid.uuid4()),
        "symbol": symbol,
        "timeframe": "1min",
        "timestamp_ms": int(time.time() * 1000) + ts_offset_s * 1000,
        "open": 100.0,
        "high": 102.0,
        "low": 99.0,
        "close": 101.0,
        "volume": 5000,
        "vwap": 100.5,
        "trade_count": 100,
        "source": "alpha_vantage",
        "is_mock": False,
    }


class TestFeatureEngineeringJob:
    def test_no_output_during_warmup(self, mock_timescaledb_sink):
        from src.jobs.feature_engineering_job import FeatureEngineeringJob

        job = FeatureEngineeringJob(db_sink=mock_timescaledb_sink)
        # Process fewer bars than FEATURE_MIN_BARS
        for i in range(settings.FEATURE_MIN_BARS - 1):
            outputs = job.process(make_bar(ts_offset_s=i), MagicMock())
            assert outputs == [], f"Expected no output at bar {i+1}, warmup not complete"

    def test_output_after_warmup(self, mock_timescaledb_sink):
        from src.jobs.feature_engineering_job import FeatureEngineeringJob

        job = FeatureEngineeringJob(db_sink=mock_timescaledb_sink)
        # Feed enough bars to complete warmup
        for i in range(settings.FEATURE_MIN_BARS):
            job.process(make_bar(ts_offset_s=i), MagicMock())

        # Now one more bar should emit indicators
        outputs = job.process(make_bar(ts_offset_s=settings.FEATURE_MIN_BARS), MagicMock())
        assert len(outputs) > 0

    def test_output_topics(self, mock_timescaledb_sink):
        from src.jobs.feature_engineering_job import FeatureEngineeringJob

        job = FeatureEngineeringJob(db_sink=mock_timescaledb_sink)
        for i in range(settings.FEATURE_MIN_BARS + 1):
            outputs = job.process(make_bar(ts_offset_s=i), MagicMock())

        for topic, key, value in outputs:
            assert topic == "technical.indicators"
            assert key == b"AAPL"
            assert len(value) > 5  # Avro wire format

    def test_db_write_called_after_warmup(self, mock_timescaledb_sink):
        from src.jobs.feature_engineering_job import FeatureEngineeringJob

        job = FeatureEngineeringJob(db_sink=mock_timescaledb_sink)
        for i in range(settings.FEATURE_MIN_BARS + 1):
            job.process(make_bar(ts_offset_s=i), MagicMock())

        assert mock_timescaledb_sink.write_technical_indicator.call_count > 0

    def test_no_db_write_during_warmup(self, mock_timescaledb_sink):
        from src.jobs.feature_engineering_job import FeatureEngineeringJob

        job = FeatureEngineeringJob(db_sink=mock_timescaledb_sink)
        for i in range(settings.FEATURE_MIN_BARS - 1):
            job.process(make_bar(ts_offset_s=i), MagicMock())

        mock_timescaledb_sink.write_technical_indicator.assert_not_called()

    def test_indicator_records_have_required_fields(self, mock_timescaledb_sink):
        from src.jobs.feature_engineering_job import FeatureEngineeringJob
        from tests.conftest import avro_decode

        job = FeatureEngineeringJob(db_sink=mock_timescaledb_sink)
        for i in range(settings.FEATURE_MIN_BARS + 1):
            outputs = job.process(make_bar(ts_offset_s=i), MagicMock())

        for topic, key, value in outputs:
            record = avro_decode("technical_indicator", value)
            assert record["symbol"] == "AAPL"
            assert record["indicator_name"] != ""
            assert record["timeframe"] == "1min"
            assert isinstance(record["value"], float)

    def test_multiple_symbols_independent(self, mock_timescaledb_sink):
        from src.jobs.feature_engineering_job import FeatureEngineeringJob

        job = FeatureEngineeringJob(db_sink=mock_timescaledb_sink)
        for i in range(settings.FEATURE_MIN_BARS + 1):
            job.process(make_bar("AAPL", ts_offset_s=i), MagicMock())
            job.process(make_bar("MSFT", ts_offset_s=i), MagicMock())

        # Each symbol should have its own state
        aapl_state = job._rolling_state.get_or_create("AAPL")
        msft_state = job._rolling_state.get_or_create("MSFT")
        assert aapl_state is not msft_state
        assert aapl_state.bar_count > 0
        assert msft_state.bar_count > 0
