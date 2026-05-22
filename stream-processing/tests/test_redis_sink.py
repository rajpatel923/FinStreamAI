"""Tests for RedisSink."""
from unittest.mock import MagicMock, patch

import pytest
import redis


class TestRedisSinkLazyConnection:
    def test_no_connection_on_init(self):
        with patch("src.sinks.redis_sink.redis.Redis"):
            from src.sinks.redis_sink import RedisSink
            sink = RedisSink(host="localhost", port=6379, ttl_s=300)
            assert sink._conn is None

    def test_connection_created_on_first_use(self):
        with patch("src.sinks.redis_sink.redis.Redis") as mock_redis_cls:
            mock_conn = MagicMock()
            mock_redis_cls.return_value = mock_conn
            from src.sinks.redis_sink import RedisSink
            sink = RedisSink(host="testhost", port=1234, password=None, ttl_s=60)
            conn = sink._get_conn()
            call_kwargs = mock_redis_cls.call_args[1]
            assert call_kwargs["host"] == "testhost"
            assert call_kwargs["port"] == 1234
            assert call_kwargs["decode_responses"] is False
            assert conn is mock_conn

    def test_connection_cached_on_subsequent_calls(self):
        with patch("src.sinks.redis_sink.redis.Redis") as mock_redis_cls:
            mock_conn = MagicMock()
            mock_redis_cls.return_value = mock_conn
            from src.sinks.redis_sink import RedisSink
            sink = RedisSink(host="localhost", port=6379, ttl_s=300)
            c1 = sink._get_conn()
            c2 = sink._get_conn()
            assert c1 is c2
            mock_redis_cls.assert_called_once()


class TestRedisSinkWriteFeatures:
    def _make_sink(self):
        with patch("src.sinks.redis_sink.redis.Redis") as mock_redis_cls:
            mock_conn = MagicMock()
            mock_redis_cls.return_value = mock_conn
            from src.sinks.redis_sink import RedisSink
            sink = RedisSink(host="localhost", port=6379, ttl_s=300)
            sink._conn = mock_conn  # pre-inject
            return sink, mock_conn

    def test_write_features_calls_hset_with_correct_key(self):
        sink, mock_conn = self._make_sink()
        sink.write_features("AAPL", {"close": 150.0, "volume": 10000})
        mock_conn.hset.assert_called_once()
        call_kwargs = mock_conn.hset.call_args
        assert call_kwargs[0][0] == "finstreami:features:AAPL"

    def test_write_features_calls_expire_with_ttl(self):
        sink, mock_conn = self._make_sink()
        sink.write_features("AAPL", {"close": 150.0})
        mock_conn.expire.assert_called_once_with("finstreami:features:AAPL", 300)

    def test_write_features_filters_none_values(self):
        sink, mock_conn = self._make_sink()
        sink.write_features("AAPL", {"close": 150.0, "news_sentiment": None, "volume": 5000})
        mapping = mock_conn.hset.call_args[1]["mapping"]
        assert "news_sentiment" not in mapping
        assert "close" in mapping
        assert "volume" in mapping

    def test_write_features_skips_empty_after_filter(self):
        sink, mock_conn = self._make_sink()
        sink.write_features("AAPL", {"a": None, "b": None})
        mock_conn.hset.assert_not_called()

    def test_write_features_converts_values_to_strings(self):
        sink, mock_conn = self._make_sink()
        sink.write_features("AAPL", {"close": 150.5, "volume": 1000})
        mapping = mock_conn.hset.call_args[1]["mapping"]
        assert mapping["close"] == "150.5"
        assert mapping["volume"] == "1000"

    def test_write_features_catches_redis_error(self):
        sink, mock_conn = self._make_sink()
        mock_conn.hset.side_effect = redis.RedisError("connection refused")
        # Should not raise
        sink.write_features("AAPL", {"close": 150.0})

    def test_write_features_increments_error_counter_on_failure(self):
        from unittest.mock import patch as mpatch
        sink, mock_conn = self._make_sink()
        mock_conn.hset.side_effect = redis.RedisError("timeout")
        with mpatch("src.sinks.redis_sink.redis_write_errors_total") as mock_counter:
            mock_labels = MagicMock()
            mock_counter.labels.return_value = mock_labels
            sink.write_features("AAPL", {"close": 150.0})
            mock_counter.labels.assert_called_with(type="features")
            mock_labels.inc.assert_called_once()

    def test_write_features_increments_success_counter(self):
        from unittest.mock import patch as mpatch
        sink, mock_conn = self._make_sink()
        with mpatch("src.sinks.redis_sink.redis_writes_total") as mock_counter:
            mock_labels = MagicMock()
            mock_counter.labels.return_value = mock_labels
            sink.write_features("AAPL", {"close": 150.0})
            mock_counter.labels.assert_called_with(type="features")
            mock_labels.inc.assert_called_once()


class TestRedisSinkWriteSentiment:
    def _make_sink(self):
        with patch("src.sinks.redis_sink.redis.Redis") as mock_redis_cls:
            mock_conn = MagicMock()
            mock_redis_cls.return_value = mock_conn
            from src.sinks.redis_sink import RedisSink
            sink = RedisSink(host="localhost", port=6379, ttl_s=300)
            sink._conn = mock_conn
            return sink, mock_conn

    def test_write_sentiment_uses_sentiment_key_prefix(self):
        sink, mock_conn = self._make_sink()
        sink.write_sentiment("MSFT", {"score": 0.5, "confidence": 0.9})
        key = mock_conn.hset.call_args[0][0]
        assert key == "finstreami:sentiment:MSFT"

    def test_write_sentiment_calls_expire(self):
        sink, mock_conn = self._make_sink()
        sink.write_sentiment("MSFT", {"score": 0.5})
        mock_conn.expire.assert_called_once_with("finstreami:sentiment:MSFT", 300)

    def test_write_sentiment_filters_none(self):
        sink, mock_conn = self._make_sink()
        sink.write_sentiment("MSFT", {"score": None})
        mock_conn.hset.assert_not_called()

    def test_write_sentiment_catches_redis_error(self):
        sink, mock_conn = self._make_sink()
        mock_conn.hset.side_effect = redis.RedisError("timeout")
        sink.write_sentiment("MSFT", {"score": 0.5})  # no raise

    def test_write_sentiment_increments_success_counter(self):
        from unittest.mock import patch as mpatch
        sink, mock_conn = self._make_sink()
        with mpatch("src.sinks.redis_sink.redis_writes_total") as mock_counter:
            mock_labels = MagicMock()
            mock_counter.labels.return_value = mock_labels
            sink.write_sentiment("MSFT", {"score": 0.5})
            mock_counter.labels.assert_called_with(type="sentiment")
            mock_labels.inc.assert_called_once()


class TestRedisSinkCloseFlush:
    def test_flush_is_noop(self):
        with patch("src.sinks.redis_sink.redis.Redis"):
            from src.sinks.redis_sink import RedisSink
            sink = RedisSink(host="localhost", port=6379, ttl_s=300)
            sink.flush()  # must not raise

    def test_close_clears_connection(self):
        with patch("src.sinks.redis_sink.redis.Redis") as mock_redis_cls:
            mock_conn = MagicMock()
            mock_redis_cls.return_value = mock_conn
            from src.sinks.redis_sink import RedisSink
            sink = RedisSink(host="localhost", port=6379, ttl_s=300)
            sink._conn = mock_conn
            sink.close()
            mock_conn.close.assert_called_once()
            assert sink._conn is None

    def test_close_with_no_connection_is_safe(self):
        with patch("src.sinks.redis_sink.redis.Redis"):
            from src.sinks.redis_sink import RedisSink
            sink = RedisSink(host="localhost", port=6379, ttl_s=300)
            sink.close()  # _conn is None, must not raise

    def test_close_swallows_exceptions(self):
        with patch("src.sinks.redis_sink.redis.Redis") as mock_redis_cls:
            mock_conn = MagicMock()
            mock_conn.close.side_effect = Exception("broken")
            mock_redis_cls.return_value = mock_conn
            from src.sinks.redis_sink import RedisSink
            sink = RedisSink(host="localhost", port=6379, ttl_s=300)
            sink._conn = mock_conn
            sink.close()  # must not raise
            assert sink._conn is None
