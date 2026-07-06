"""Tests for FinnhubProducer (WebSocket) and MarketDataProducer Finnhub REST fallback."""
import json
import queue
import time
import uuid
from unittest.mock import MagicMock, patch

import pytest

from tests.conftest import avro_decode


# ─── Helpers ─────────────────────────────────────────────────────────────────

def _make_trade_message(symbol: str = "AAPL", price: float = 182.0, volume: int = 100) -> str:
    """Minimal Finnhub WebSocket trade message."""
    return json.dumps({
        "type": "trade",
        "data": [{"s": symbol, "p": price, "t": int(time.time() * 1000), "v": volume}],
    })


def _make_tick(symbol: str = "AAPL", price: float = 182.0, source: str = "finnhub") -> dict:
    return {
        "event_id": str(uuid.uuid4()),
        "symbol": symbol,
        "timestamp_ms": int(time.time() * 1000),
        "price": price,
        "volume": 100,
        "bid_price": None,
        "ask_price": None,
        "bid_size": None,
        "ask_size": None,
        "exchange": None,
        "source": source,
        "is_mock": False,
    }


# ─── Fixtures ─────────────────────────────────────────────────────────────────

@pytest.fixture
def finnhub_producer(mock_kafka_producer):
    with patch.dict("os.environ", {"FINNHUB_API_KEY": "test_key"}, clear=False):
        from src.producers.finnhub_producer import FinnhubProducer
        p = FinnhubProducer()
        p._api_key = "test_key"
        p._symbols = ["AAPL", "MSFT"]
        return p


# ─── FinnhubProducer: metadata ────────────────────────────────────────────────

class TestFinnhubProducerMetadata:
    def test_data_source_name(self, finnhub_producer):
        assert finnhub_producer.data_source_name() == "finnhub_ws"

    def test_poll_interval_is_fast(self, finnhub_producer):
        assert finnhub_producer.poll_interval_seconds() < 1.0

    def test_topic(self, finnhub_producer):
        assert finnhub_producer.topic == "market.ticks.raw"


# ─── FinnhubProducer: WebSocket message parsing ───────────────────────────────

class TestFinnhubProducerOnMessage:
    def test_trade_message_enqueued(self, finnhub_producer):
        finnhub_producer._on_message(None, _make_trade_message("AAPL", 182.5, 200))
        assert finnhub_producer._tick_queue.qsize() == 1

    def test_enqueued_tick_fields(self, finnhub_producer):
        finnhub_producer._on_message(None, _make_trade_message("MSFT", 375.0, 50))
        tick = finnhub_producer._tick_queue.get_nowait()
        assert tick["symbol"] == "MSFT"
        assert tick["price"] == 375.0
        assert tick["volume"] == 50
        assert tick["source"] == "finnhub"
        assert tick["is_mock"] is False
        assert "event_id" in tick

    def test_non_trade_message_ignored(self, finnhub_producer):
        finnhub_producer._on_message(None, json.dumps({"type": "ping"}))
        assert finnhub_producer._tick_queue.empty()

    def test_multiple_trades_in_one_message(self, finnhub_producer):
        msg = json.dumps({
            "type": "trade",
            "data": [
                {"s": "AAPL", "p": 182.0, "t": 1700000000000, "v": 100},
                {"s": "MSFT", "p": 375.0, "t": 1700000000001, "v": 50},
            ],
        })
        finnhub_producer._on_message(None, msg)
        assert finnhub_producer._tick_queue.qsize() == 2

    def test_malformed_json_ignored(self, finnhub_producer):
        finnhub_producer._on_message(None, "not-valid-json{{")
        assert finnhub_producer._tick_queue.empty()

    def test_missing_symbol_skipped(self, finnhub_producer):
        msg = json.dumps({"type": "trade", "data": [{"p": 100.0, "t": 1700000000000, "v": 10}]})
        finnhub_producer._on_message(None, msg)
        assert finnhub_producer._tick_queue.empty()

    def test_queue_full_does_not_raise(self, finnhub_producer):
        finnhub_producer._tick_queue = queue.Queue(maxsize=1)
        finnhub_producer._on_message(None, _make_trade_message("AAPL", 182.0))
        # second message should be silently dropped
        finnhub_producer._on_message(None, _make_trade_message("MSFT", 375.0))
        assert finnhub_producer._tick_queue.qsize() == 1


# ─── FinnhubProducer: fetch_data queue drain ─────────────────────────────────

class TestFinnhubProducerFetchData:
    def test_fetch_data_drains_queue(self, finnhub_producer):
        for sym in ["AAPL", "MSFT", "GOOGL"]:
            finnhub_producer._tick_queue.put(_make_tick(sym))
        result = finnhub_producer.fetch_data()
        assert len(result) == 3
        assert finnhub_producer._tick_queue.empty()

    def test_fetch_data_empty_queue(self, finnhub_producer):
        assert finnhub_producer.fetch_data() == []

    def test_fetch_data_symbols_preserved(self, finnhub_producer):
        finnhub_producer._tick_queue.put(_make_tick("NVDA", 870.0))
        result = finnhub_producer.fetch_data()
        assert result[0]["symbol"] == "NVDA"
        assert result[0]["price"] == 870.0


# ─── FinnhubProducer: transform / Avro ───────────────────────────────────────

class TestFinnhubProducerTransform:
    def test_transform_valid_tick(self, finnhub_producer):
        results = finnhub_producer.transform([_make_tick("AAPL", 182.5)])
        assert len(results) == 1
        key, value = results[0]
        assert key == "AAPL"
        assert isinstance(value, bytes)

    def test_transform_avro_round_trip(self, finnhub_producer):
        tick = _make_tick("MSFT", 375.0)
        results = finnhub_producer.transform([tick])
        decoded = avro_decode("market_tick", results[0][1])
        assert decoded["symbol"] == "MSFT"
        assert decoded["price"] == 375.0
        assert decoded["source"] == "finnhub"

    def test_transform_rejects_invalid_price(self, finnhub_producer):
        bad = _make_tick("AAPL", -10.0)
        assert finnhub_producer.transform([bad]) == []

    def test_transform_rejects_empty_symbol(self, finnhub_producer):
        bad = _make_tick("")
        assert finnhub_producer.transform([bad]) == []

    def test_transform_empty_input(self, finnhub_producer):
        assert finnhub_producer.transform([]) == []


# ─── MarketDataProducer: Finnhub REST fallback ────────────────────────────────

class TestMarketDataProducerFinnhubFallback:
    @pytest.fixture
    def finnhub_rest_producer(self, mock_kafka_producer):
        with patch.dict("os.environ", {"USE_MOCK_DATA": "true"}, clear=False):
            from src.producers.market_data_producer import MarketDataProducer
            p = MarketDataProducer()
        # Simulate Finnhub-only config (no Polygon/AV keys)
        p._use_mock = False
        p._polygon_key = ""
        p._av_key = ""
        p._finnhub_key = "test_fh_key"
        p._symbols = ["AAPL"]
        return p

    def test_data_source_is_finnhub(self, finnhub_rest_producer):
        assert finnhub_rest_producer.data_source_name() == "finnhub"

    def test_poll_interval_finnhub(self, finnhub_rest_producer):
        assert finnhub_rest_producer.poll_interval_seconds() == 10.0

    def test_fetch_finnhub_maps_fields(self, finnhub_rest_producer):
        quote = {"c": 183.25, "h": 185.0, "l": 181.0, "o": 182.0, "pc": 181.5, "t": 1700000000}
        mock_resp = MagicMock()
        mock_resp.json.return_value = quote
        mock_resp.raise_for_status = MagicMock()
        finnhub_rest_producer._http = MagicMock()
        finnhub_rest_producer._http.get.return_value = mock_resp

        result = finnhub_rest_producer._fetch_finnhub()
        assert len(result) == 1
        tick = result[0]
        assert tick["symbol"] == "AAPL"
        assert tick["price"] == 183.25
        assert tick["source"] == "finnhub"
        assert tick["is_mock"] is False
        assert tick["timestamp_ms"] == 1700000000 * 1000

    def test_fetch_finnhub_handles_error(self, finnhub_rest_producer):
        finnhub_rest_producer._http = MagicMock()
        finnhub_rest_producer._http.get.side_effect = Exception("network error")
        result = finnhub_rest_producer._fetch_finnhub()
        assert result == []

    def test_no_mock_when_finnhub_key_set(self, finnhub_rest_producer):
        assert finnhub_rest_producer._use_mock is False
