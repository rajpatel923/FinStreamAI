"""Tests for Avro schema loading and serialization round-trips."""
import io
import json
import struct
import time
import uuid
from pathlib import Path

import fastavro
import pytest

from tests.conftest import avro_decode, avro_encode, load_schema


class TestSchemaLoading:
    def test_market_tick_schema_loads(self):
        schema = load_schema("market_tick")
        assert schema["name"] == "MarketTick"
        field_names = {f["name"] for f in schema["fields"]}
        assert {"event_id", "symbol", "price", "volume", "is_mock"} <= field_names

    def test_market_bar_schema_loads(self):
        schema = load_schema("market_bar")
        assert schema["name"] == "MarketBar"
        field_names = {f["name"] for f in schema["fields"]}
        assert {"open", "high", "low", "close", "volume"} <= field_names

    def test_news_article_schema_loads(self):
        schema = load_schema("news_article")
        assert schema["name"] == "NewsArticle"

    def test_social_post_schema_loads(self):
        schema = load_schema("social_post")
        assert schema["name"] == "SocialPost"
        field_names = {f["name"] for f in schema["fields"]}
        assert "subreddit" in field_names

    def test_sec_filing_schema_loads(self):
        schema = load_schema("sec_filing")
        assert schema["name"] == "SECFiling"
        field_names = {f["name"] for f in schema["fields"]}
        assert "cik" in field_names and "form_type" in field_names


class TestRoundTrip:
    def test_market_tick_round_trip(self, sample_tick):
        encoded = avro_encode("market_tick", sample_tick)
        # Verify Confluent magic header
        assert encoded[0] == 0
        decoded = avro_decode("market_tick", encoded)
        assert decoded["symbol"] == sample_tick["symbol"]
        assert abs(decoded["price"] - sample_tick["price"]) < 1e-6
        assert decoded["volume"] == sample_tick["volume"]
        assert decoded["is_mock"] == False

    def test_market_bar_round_trip(self, sample_bar):
        encoded = avro_encode("market_bar", sample_bar)
        decoded = avro_decode("market_bar", encoded)
        assert decoded["symbol"] == sample_bar["symbol"]
        assert decoded["timeframe"] == "1min"
        assert abs(decoded["close"] - sample_bar["close"]) < 1e-6

    def test_news_article_round_trip(self, sample_article):
        encoded = avro_encode("news_article", sample_article)
        decoded = avro_decode("news_article", encoded)
        assert decoded["headline"] == sample_article["headline"]
        assert decoded["symbols"] == ["AAPL"]

    def test_social_post_round_trip(self, sample_post):
        encoded = avro_encode("social_post", sample_post)
        decoded = avro_decode("social_post", encoded)
        assert decoded["post_id"] == sample_post["post_id"]
        assert decoded["platform"] == "reddit"

    def test_sec_filing_round_trip(self, sample_filing):
        encoded = avro_encode("sec_filing", sample_filing)
        decoded = avro_decode("sec_filing", encoded)
        assert decoded["cik"] == sample_filing["cik"]
        assert decoded["form_type"] == "8-K"

    def test_null_optional_fields(self):
        tick = {
            "event_id": str(uuid.uuid4()),
            "symbol": "TSLA",
            "timestamp_ms": int(time.time() * 1000),
            "price": 245.0,
            "volume": 500,
            "bid_price": None,
            "ask_price": None,
            "bid_size": None,
            "ask_size": None,
            "exchange": None,
            "source": "polygon",
            "is_mock": False,
        }
        encoded = avro_encode("market_tick", tick)
        decoded = avro_decode("market_tick", encoded)
        assert decoded["bid_price"] is None
        assert decoded["exchange"] is None

    def test_wire_format_has_5_byte_header(self, sample_tick):
        encoded = avro_encode("market_tick", sample_tick)
        magic = encoded[0]
        schema_id = struct.unpack(">I", encoded[1:5])[0]
        assert magic == 0
        assert schema_id == 1


class TestAvroSerializer:
    def test_serialize_returns_bytes(self, sample_tick):
        from src.schemas.registry import avro_serializer
        result = avro_serializer.serialize("market_tick", sample_tick)
        assert isinstance(result, bytes)
        assert len(result) > 5

    def test_deserialize_round_trip(self, sample_tick):
        from src.schemas.registry import avro_serializer
        encoded = avro_serializer.serialize("market_tick", sample_tick)
        decoded = avro_serializer.deserialize("market_tick", encoded)
        assert decoded["symbol"] == sample_tick["symbol"]

    def test_schema_cached_after_first_load(self):
        from src.schemas.registry import AvroSerializer
        ser = AvroSerializer("http://localhost:8081")
        ser._load_schema("market_tick")
        ser._load_schema("market_tick")  # second call uses cache
        assert "market_tick" in ser._schemas
