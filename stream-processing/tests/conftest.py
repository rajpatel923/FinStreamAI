import io
import json
import struct
import time
import uuid
from pathlib import Path
from unittest.mock import MagicMock, patch

import fastavro
import pytest

# ─── Schema helpers ─────────────────────────────────────────────────────────────

_SCHEMA_DIR = Path(__file__).parent.parent / "src" / "schemas" / "avro"


def load_schema(name: str) -> dict:
    with (_SCHEMA_DIR / f"{name}.avsc").open() as f:
        return json.load(f)


def avro_encode(schema_name: str, record: dict, schema_id: int = 1) -> bytes:
    schema = fastavro.parse_schema(load_schema(schema_name))
    buf = io.BytesIO()
    buf.write(struct.pack(">bI", 0, schema_id))
    fastavro.schemaless_writer(buf, schema, record)
    return buf.getvalue()


def avro_decode(schema_name: str, data: bytes) -> dict:
    schema = fastavro.parse_schema(load_schema(schema_name))
    buf = io.BytesIO(data)
    buf.read(5)  # skip magic + schema_id
    return fastavro.schemaless_reader(buf, schema)


# ─── Sample records ─────────────────────────────────────────────────────────────

@pytest.fixture
def sample_tick() -> dict:
    return {
        "event_id": str(uuid.uuid4()),
        "symbol": "AAPL",
        "timestamp_ms": int(time.time() * 1000),
        "price": 182.50,
        "volume": 1000,
        "bid_price": 182.48,
        "ask_price": 182.52,
        "bid_size": 500,
        "ask_size": 600,
        "exchange": "NASDAQ",
        "source": "polygon",
        "is_mock": False,
    }


@pytest.fixture
def sample_bar() -> dict:
    return {
        "bar_id": str(uuid.uuid4()),
        "symbol": "AAPL",
        "timeframe": "1min",
        "timestamp_ms": int(time.time() * 1000),
        "open": 182.0,
        "high": 183.5,
        "low": 181.8,
        "close": 183.2,
        "volume": 50000,
        "vwap": 182.9,
        "trade_count": 450,
        "source": "alpha_vantage",
        "is_mock": False,
    }


@pytest.fixture
def sample_indicator() -> dict:
    return {
        "indicator_id": str(uuid.uuid4()),
        "symbol": "AAPL",
        "timestamp_ms": int(time.time() * 1000),
        "indicator_name": "RSI14",
        "timeframe": "1min",
        "value": 45.0,
        "signal_value": None,
        "upper_band": None,
        "lower_band": None,
        "metadata": {},
    }


# ─── Mock Kafka consumer ─────────────────────────────────────────────────────────

def make_mock_message(value_bytes: bytes, topic: str = "test.topic", symbol: str = "AAPL"):
    msg = MagicMock()
    msg.value.return_value = value_bytes
    msg.topic.return_value = topic
    msg.error.return_value = None
    msg.key.return_value = symbol.encode()
    return msg


@pytest.fixture
def mock_kafka_consumer():
    with patch("src.jobs.base_job.Consumer") as mock_cls:
        mock_instance = MagicMock()
        mock_instance.poll.return_value = None  # no messages by default
        mock_cls.return_value = mock_instance
        yield mock_instance


@pytest.fixture
def mock_kafka_producer():
    with patch("src.sinks.kafka_sink.Producer") as mock_cls:
        mock_instance = MagicMock()
        mock_cls.return_value = mock_instance
        yield mock_instance


@pytest.fixture
def mock_timescaledb_sink():
    sink = MagicMock()
    sink.write_market_bar = MagicMock()
    sink.write_technical_indicator = MagicMock()
    sink.write_trading_signal = MagicMock()
    sink.flush = MagicMock()
    sink.close = MagicMock()
    return sink


# ─── Patch Schema Registry ───────────────────────────────────────────────────────

@pytest.fixture(autouse=True)
def patch_schema_registry():
    with patch("src.utils.avro_utils.httpx.post") as mock_post:
        mock_post.return_value = MagicMock(
            status_code=200,
            json=lambda: {"id": 1},
            raise_for_status=lambda: None,
        )
        yield mock_post
