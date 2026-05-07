import io
import json
import struct
import time
import uuid
from pathlib import Path
from unittest.mock import MagicMock, patch

import fastavro
import pytest


# ─── Schema helpers ────────────────────────────────────────────────────────────

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


# ─── Sample records ────────────────────────────────────────────────────────────

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
        "symbol": "MSFT",
        "timeframe": "1min",
        "timestamp_ms": int(time.time() * 1000),
        "open": 374.0,
        "high": 376.5,
        "low": 373.8,
        "close": 375.2,
        "volume": 50000,
        "vwap": 374.9,
        "trade_count": 450,
        "source": "alpha_vantage",
        "is_mock": False,
    }


@pytest.fixture
def sample_article() -> dict:
    return {
        "article_id": str(uuid.uuid4()),
        "headline": "Apple beats Q3 earnings expectations",
        "body": "Apple Inc. reported record revenue...",
        "url": "https://example.com/apple-earnings",
        "source": "Reuters",
        "published_ms": int(time.time() * 1000) - 60_000,
        "ingested_ms": int(time.time() * 1000),
        "symbols": ["AAPL"],
        "language": "en",
        "is_mock": False,
    }


@pytest.fixture
def sample_post() -> dict:
    return {
        "post_id": "abc123",
        "platform": "reddit",
        "subreddit": "wallstreetbets",
        "title": "$AAPL to the moon! DD inside",
        "body": "Here's my thesis...",
        "author": "trader42",
        "score": 1500,
        "num_comments": 200,
        "created_ms": int(time.time() * 1000),
        "symbols": ["AAPL"],
        "url": "https://reddit.com/r/wallstreetbets/comments/abc123",
        "is_mock": False,
    }


@pytest.fixture
def sample_filing() -> dict:
    return {
        "filing_id": "0000320193-24-000123",
        "cik": "0000320193",
        "company_name": "Apple Inc.",
        "ticker": "AAPL",
        "form_type": "8-K",
        "filed_ms": int(time.time() * 1000),
        "period_of_report": "2024-03-31",
        "filing_url": "https://www.sec.gov/Archives/edgar/data/320193/filing.htm",
        "description": "Current report on Form 8-K",
        "is_mock": False,
    }


# ─── Mock Kafka producer ───────────────────────────────────────────────────────

@pytest.fixture
def mock_kafka_producer():
    with patch("src.producers.base_producer.Producer") as mock_cls:
        mock_instance = MagicMock()
        mock_cls.return_value = mock_instance
        yield mock_instance


# ─── Patch Schema Registry so no HTTP calls are made in tests ─────────────────

@pytest.fixture(autouse=True)
def patch_schema_registry():
    with patch("src.schemas.registry.httpx.post") as mock_post:
        mock_post.return_value = MagicMock(
            status_code=200,
            json=lambda: {"id": 1},
            raise_for_status=lambda: None,
        )
        yield mock_post
