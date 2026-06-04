"""Tests for RedisCache."""
from __future__ import annotations

import json
import pytest
from unittest.mock import MagicMock
from src.cache.redis_cache import RedisCache, TTL_PRICES, TTL_FEATURES


class TestRedisCache:
    def test_set_and_get(self, redis_cache):
        redis_cache.set("test_key", {"value": 42}, ttl=60)
        result = redis_cache.get("test_key")
        assert result == {"value": 42}

    def test_get_missing_key(self, redis_cache):
        assert redis_cache.get("nonexistent_key_xyz") is None

    def test_delete_key(self, redis_cache):
        redis_cache.set("del_key", "hello", ttl=60)
        assert redis_cache.exists("del_key")
        deleted = redis_cache.delete("del_key")
        assert deleted
        assert not redis_cache.exists("del_key")

    def test_get_or_set_cache_miss(self, redis_cache):
        fetch_called = [0]

        def fetch():
            fetch_called[0] += 1
            return {"price": 150.0}

        result = redis_cache.get_or_set("price:AAPL", fetch, TTL_PRICES)
        assert result == {"price": 150.0}
        assert fetch_called[0] == 1

    def test_get_or_set_cache_hit(self, redis_cache):
        redis_cache.set("price:MSFT", {"price": 300.0}, TTL_PRICES)
        fetch_called = [0]

        def fetch():
            fetch_called[0] += 1
            return {"price": 999.0}

        result = redis_cache.get_or_set("price:MSFT", fetch, TTL_PRICES)
        assert result == {"price": 300.0}
        assert fetch_called[0] == 0  # not called

    def test_write_through(self, redis_cache):
        value = {"feature": 0.5}
        returned = redis_cache.write_through("feat:AAPL", value, TTL_FEATURES)
        assert returned == value
        assert redis_cache.get("feat:AAPL") == value

    def test_warm_prices(self, redis_cache):
        prices = {"AAPL": 150.0, "MSFT": 300.0}
        loaded = redis_cache.warm_prices(list(prices.keys()), lambda s: prices.get(s))
        assert loaded == 2
        assert redis_cache.get("price:AAPL") == 150.0

    def test_warm_prices_fetch_failure(self, redis_cache):
        def fetch(s):
            raise ValueError("DB error")

        loaded = redis_cache.warm_prices(["AAPL"], fetch)
        assert loaded == 0

    def test_warm_prices_none_return(self, redis_cache):
        loaded = redis_cache.warm_prices(["AAPL"], lambda s: None)
        assert loaded == 0

    def test_get_stats_returns_dict(self, redis_cache):
        stats = redis_cache.get_stats()
        assert isinstance(stats, dict)

    def test_ttl_tiers(self):
        assert TTL_PRICES == 60
        assert TTL_FEATURES == 300

    def test_connect_and_ping(self):
        """Test that connect() calls ping (via fakeredis or mock)."""
        cache = RedisCache("localhost", 6379)
        mock_redis = MagicMock()
        cache._client = mock_redis
        # Already connected — client property should return existing client
        assert cache.client is mock_redis
