"""Tests for MarketDataProducer — mock mode, transform, validation."""
import time
import uuid
from unittest.mock import MagicMock, patch

import pytest

from tests.conftest import avro_decode


@pytest.fixture
def mock_producer(mock_kafka_producer):
    with patch.dict("os.environ", {"USE_MOCK_DATA": "true"}, clear=False):
        from src.producers.market_data_producer import MarketDataProducer
        p = MarketDataProducer()
        p._use_mock = True
        return p


class TestMarketDataProducerMockMode:
    def test_fetch_data_returns_list(self, mock_producer):
        data = mock_producer.fetch_data()
        assert isinstance(data, list)
        assert len(data) > 0

    def test_fetched_ticks_have_required_fields(self, mock_producer):
        data = mock_producer.fetch_data()
        required = {"event_id", "symbol", "timestamp_ms", "price", "volume", "is_mock"}
        for tick in data:
            assert required <= tick.keys(), f"Missing fields in tick: {tick.keys()}"

    def test_fetched_ticks_marked_as_mock(self, mock_producer):
        data = mock_producer.fetch_data()
        assert all(tick["is_mock"] for tick in data)

    def test_transform_produces_avro_bytes(self, mock_producer):
        data = mock_producer.fetch_data()
        results = mock_producer.transform(data)
        assert len(results) > 0
        for key, value in results:
            assert isinstance(value, bytes)
            assert len(value) > 5

    def test_transform_key_is_symbol(self, mock_producer):
        data = mock_producer.fetch_data()
        results = mock_producer.transform(data)
        for key, value in results:
            assert key is not None
            assert isinstance(key, str)
            assert len(key) >= 1

    def test_transform_avro_round_trip(self, mock_producer):
        data = mock_producer.fetch_data()
        results = mock_producer.transform(data)
        for key, value in results:
            decoded = avro_decode("market_tick", value)
            assert decoded["price"] > 0
            assert decoded["volume"] >= 0
            assert decoded["symbol"] == key

    def test_transform_rejects_invalid_price(self, mock_producer):
        bad_tick = mock_producer.fetch_data()[0].copy()
        bad_tick["price"] = -1.0
        results = mock_producer.transform([bad_tick])
        assert results == []

    def test_transform_rejects_empty_symbol(self, mock_producer):
        bad_tick = mock_producer.fetch_data()[0].copy()
        bad_tick["symbol"] = ""
        results = mock_producer.transform([bad_tick])
        assert results == []

    def test_data_source_name_is_mock(self, mock_producer):
        assert mock_producer.data_source_name() == "mock"

    def test_poll_interval_mock_mode(self, mock_producer):
        assert mock_producer.poll_interval_seconds() == 1.0
