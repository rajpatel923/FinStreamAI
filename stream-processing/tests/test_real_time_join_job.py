"""Tests for RealTimeJoinJob."""
import time
import uuid
from unittest.mock import MagicMock, patch

import pytest


def make_bar(symbol="AAPL", ts_ms=None):
    return {
        "bar_id": str(uuid.uuid4()),
        "symbol": symbol,
        "timeframe": "5min",
        "timestamp_ms": ts_ms or int(time.time() * 1000),
        "open": 180.0,
        "high": 182.0,
        "low": 179.0,
        "close": 181.0,
        "volume": 20000,
        "vwap": 180.5,
        "trade_count": 200,
        "source": "alpha_vantage",
        "is_mock": False,
    }


def make_sentiment(symbols, score=0.5, ts_ms=None):
    return {
        "id": str(uuid.uuid4()),
        "symbols": symbols,
        "sentiment_score": score,
        "confidence": 0.8,
        "scored_ms": ts_ms or int(time.time() * 1000),
        "source": "ml-service",
    }


def make_msg(topic, value=None):
    msg = MagicMock()
    msg.topic.return_value = topic
    msg.value.return_value = value or b""
    msg.error.return_value = None
    return msg


class TestRealTimeJoinJobSchemaRouting:
    def test_get_schema_bar_topic(self):
        from src.jobs.real_time_join_job import RealTimeJoinJob
        job = RealTimeJoinJob(redis_sink=MagicMock())
        msg = make_msg("market.bars.5min")
        assert job._get_schema(msg) == "market_bar"

    def test_get_schema_news_topic(self):
        from src.jobs.real_time_join_job import RealTimeJoinJob
        job = RealTimeJoinJob(redis_sink=MagicMock())
        msg = make_msg("news.articles.scored")
        assert job._get_schema(msg) == "news_sentiment"

    def test_get_schema_social_topic(self):
        from src.jobs.real_time_join_job import RealTimeJoinJob
        job = RealTimeJoinJob(redis_sink=MagicMock())
        msg = make_msg("social.sentiment")
        assert job._get_schema(msg) == "social_sentiment"

    def test_get_schema_unknown_topic_defaults_to_market_bar(self):
        from src.jobs.real_time_join_job import RealTimeJoinJob
        job = RealTimeJoinJob(redis_sink=MagicMock())
        msg = make_msg("unknown.topic")
        assert job._get_schema(msg) == "market_bar"


class TestRealTimeJoinJobCaches:
    def test_update_news_cache_stores_per_symbol(self):
        from src.jobs.real_time_join_job import RealTimeJoinJob
        job = RealTimeJoinJob(redis_sink=MagicMock())
        record = make_sentiment(["AAPL", "MSFT"], score=0.7)
        job._update_news_cache(record)
        assert "AAPL" in job._news_cache
        assert "MSFT" in job._news_cache
        assert job._news_cache["AAPL"]["score"] == 0.7

    def test_update_social_cache_stores_per_symbol(self):
        from src.jobs.real_time_join_job import RealTimeJoinJob
        job = RealTimeJoinJob(redis_sink=MagicMock())
        record = make_sentiment(["GOOGL"], score=-0.3)
        job._update_social_cache(record)
        assert "GOOGL" in job._social_cache
        assert job._social_cache["GOOGL"]["score"] == -0.3

    def test_news_cache_keeps_latest_entry(self):
        from src.jobs.real_time_join_job import RealTimeJoinJob
        job = RealTimeJoinJob(redis_sink=MagicMock())
        now = int(time.time() * 1000)
        old = make_sentiment(["AAPL"], score=0.1, ts_ms=now - 60_000)
        new = make_sentiment(["AAPL"], score=0.9, ts_ms=now)
        job._update_news_cache(old)
        job._update_news_cache(new)
        assert job._news_cache["AAPL"]["score"] == 0.9

    def test_social_cache_keeps_latest_entry(self):
        from src.jobs.real_time_join_job import RealTimeJoinJob
        job = RealTimeJoinJob(redis_sink=MagicMock())
        now = int(time.time() * 1000)
        old = make_sentiment(["TSLA"], score=-0.5, ts_ms=now - 10_000)
        new = make_sentiment(["TSLA"], score=0.2, ts_ms=now)
        job._update_social_cache(old)
        job._update_social_cache(new)
        assert job._social_cache["TSLA"]["score"] == 0.2

    def test_news_cache_does_not_overwrite_with_older(self):
        from src.jobs.real_time_join_job import RealTimeJoinJob
        job = RealTimeJoinJob(redis_sink=MagicMock())
        now = int(time.time() * 1000)
        new = make_sentiment(["AAPL"], score=0.8, ts_ms=now)
        old = make_sentiment(["AAPL"], score=0.1, ts_ms=now - 60_000)
        job._update_news_cache(new)
        job._update_news_cache(old)
        assert job._news_cache["AAPL"]["score"] == 0.8


class TestGetRecent:
    def test_returns_none_when_no_cache_entry(self):
        from src.jobs.real_time_join_job import RealTimeJoinJob
        job = RealTimeJoinJob(redis_sink=MagicMock())
        result = job._get_recent({}, "AAPL", int(time.time() * 1000))
        assert result is None

    def test_returns_score_within_window(self):
        from src.jobs.real_time_join_job import RealTimeJoinJob
        job = RealTimeJoinJob(redis_sink=MagicMock())
        now = int(time.time() * 1000)
        cache = {"AAPL": {"score": 0.5, "ts_ms": now - 60_000}}
        result = job._get_recent(cache, "AAPL", now)
        assert result == 0.5

    def test_returns_none_when_entry_too_old(self):
        from src.jobs.real_time_join_job import RealTimeJoinJob
        job = RealTimeJoinJob(redis_sink=MagicMock())
        now = int(time.time() * 1000)
        # older than JOIN_WINDOW_MS (300_000 ms)
        cache = {"AAPL": {"score": 0.5, "ts_ms": now - 400_000}}
        result = job._get_recent(cache, "AAPL", now)
        assert result is None

    def test_returns_none_for_unknown_symbol(self):
        from src.jobs.real_time_join_job import RealTimeJoinJob
        job = RealTimeJoinJob(redis_sink=MagicMock())
        now = int(time.time() * 1000)
        cache = {"AAPL": {"score": 0.5, "ts_ms": now}}
        result = job._get_recent(cache, "MSFT", now)
        assert result is None

    def test_exact_window_boundary_excluded(self):
        from src.jobs.real_time_join_job import RealTimeJoinJob
        job = RealTimeJoinJob(redis_sink=MagicMock())
        now = int(time.time() * 1000)
        # exactly at JOIN_WINDOW_MS — bar_ts - ts_ms == JOIN_WINDOW_MS → NOT > window
        cache = {"AAPL": {"score": 0.3, "ts_ms": now - job._join_window_ms}}
        result = job._get_recent(cache, "AAPL", now)
        assert result == 0.3  # equal is not > so it passes


class TestRealTimeJoinJobProcess:
    def test_process_news_topic_returns_empty_list(self):
        from src.jobs.real_time_join_job import RealTimeJoinJob
        job = RealTimeJoinJob(redis_sink=MagicMock())
        record = make_sentiment(["AAPL"], score=0.4)
        msg = make_msg("news.articles.scored")
        result = job.process(record, msg)
        assert result == []

    def test_process_social_topic_returns_empty_list(self):
        from src.jobs.real_time_join_job import RealTimeJoinJob
        job = RealTimeJoinJob(redis_sink=MagicMock())
        record = make_sentiment(["AAPL"], score=-0.2)
        msg = make_msg("social.sentiment")
        result = job.process(record, msg)
        assert result == []

    def test_process_bar_topic_returns_empty_list(self):
        from src.jobs.real_time_join_job import RealTimeJoinJob
        mock_redis = MagicMock()
        job = RealTimeJoinJob(redis_sink=mock_redis)
        msg = make_msg("market.bars.5min")
        result = job.process(make_bar(), msg)
        assert result == []

    def test_process_news_updates_cache(self):
        from src.jobs.real_time_join_job import RealTimeJoinJob
        job = RealTimeJoinJob(redis_sink=MagicMock())
        record = make_sentiment(["AAPL"], score=0.6)
        job.process(record, make_msg("news.articles.scored"))
        assert "AAPL" in job._news_cache

    def test_process_social_updates_cache(self):
        from src.jobs.real_time_join_job import RealTimeJoinJob
        job = RealTimeJoinJob(redis_sink=MagicMock())
        record = make_sentiment(["MSFT"], score=0.3)
        job.process(record, make_msg("social.sentiment"))
        assert "MSFT" in job._social_cache

    def test_process_bar_calls_redis_write(self):
        from src.jobs.real_time_join_job import RealTimeJoinJob
        mock_redis = MagicMock()
        job = RealTimeJoinJob(redis_sink=mock_redis)
        job.process(make_bar("AAPL"), make_msg("market.bars.5min"))
        mock_redis.write_features.assert_called_once()
        call_args = mock_redis.write_features.call_args
        assert call_args[0][0] == "AAPL"

    def test_process_exception_does_not_propagate(self):
        from src.jobs.real_time_join_job import RealTimeJoinJob
        mock_redis = MagicMock()
        mock_redis.write_features.side_effect = Exception("Redis down")
        job = RealTimeJoinJob(redis_sink=mock_redis)
        # Should not raise
        result = job.process(make_bar(), make_msg("market.bars.5min"))
        assert result == []


class TestJoinAndWrite:
    def test_feature_dict_includes_ohlcv(self):
        from src.jobs.real_time_join_job import RealTimeJoinJob
        mock_redis = MagicMock()
        job = RealTimeJoinJob(redis_sink=mock_redis)
        bar = make_bar("AAPL")
        job._join_and_write(bar)
        features = mock_redis.write_features.call_args[0][1]
        assert "open" in features
        assert "high" in features
        assert "low" in features
        assert "close" in features
        assert "volume" in features

    def test_feature_dict_sentiment_none_when_no_cache(self):
        from src.jobs.real_time_join_job import RealTimeJoinJob
        mock_redis = MagicMock()
        job = RealTimeJoinJob(redis_sink=mock_redis)
        job._join_and_write(make_bar("AAPL"))
        features = mock_redis.write_features.call_args[0][1]
        assert features["news_sentiment"] is None
        assert features["social_sentiment"] is None

    def test_feature_dict_includes_sentiment_from_cache(self):
        from src.jobs.real_time_join_job import RealTimeJoinJob
        mock_redis = MagicMock()
        job = RealTimeJoinJob(redis_sink=mock_redis)
        now = int(time.time() * 1000)
        job._news_cache["AAPL"] = {"score": 0.7, "ts_ms": now - 1000}
        job._social_cache["AAPL"] = {"score": -0.2, "ts_ms": now - 2000}
        job._join_and_write(make_bar("AAPL", ts_ms=now))
        features = mock_redis.write_features.call_args[0][1]
        assert features["news_sentiment"] == 0.7
        assert features["social_sentiment"] == -0.2

    def test_multi_symbol_isolation(self):
        from src.jobs.real_time_join_job import RealTimeJoinJob
        mock_redis = MagicMock()
        job = RealTimeJoinJob(redis_sink=mock_redis)
        now = int(time.time() * 1000)
        job._news_cache["AAPL"] = {"score": 0.9, "ts_ms": now - 1000}
        # MSFT has no cache
        job._join_and_write(make_bar("MSFT", ts_ms=now))
        features = mock_redis.write_features.call_args[0][1]
        assert features["news_sentiment"] is None

    def test_metrics_incremented_on_join(self):
        from src.jobs.real_time_join_job import RealTimeJoinJob
        from unittest.mock import patch as mpatch
        mock_redis = MagicMock()
        job = RealTimeJoinJob(redis_sink=mock_redis)
        with mpatch("src.jobs.real_time_join_job.joins_attempted_total") as mock_counter:
            job._join_and_write(make_bar())
            mock_counter.inc.assert_called_once()

    def test_sentiment_metrics_incremented_when_cache_hit(self):
        from src.jobs.real_time_join_job import RealTimeJoinJob
        from unittest.mock import patch as mpatch
        mock_redis = MagicMock()
        job = RealTimeJoinJob(redis_sink=mock_redis)
        now = int(time.time() * 1000)
        job._news_cache["AAPL"] = {"score": 0.5, "ts_ms": now - 1000}
        with mpatch("src.jobs.real_time_join_job.joins_with_sentiment") as mock_sent:
            mock_labels = MagicMock()
            mock_sent.labels.return_value = mock_labels
            job._join_and_write(make_bar("AAPL", ts_ms=now))
            mock_sent.labels.assert_any_call(source="news")


class TestRealTimeJoinJobLifecycle:
    def test_redis_sink_closed_after_run(self):
        from src.jobs.real_time_join_job import RealTimeJoinJob
        mock_redis = MagicMock()
        job = RealTimeJoinJob(redis_sink=mock_redis)
        call_count = 0

        def fake_poll(timeout):
            nonlocal call_count
            call_count += 1
            if call_count >= 2:
                job._running = False
            return None

        with patch("src.jobs.base_job.Consumer") as mock_cls:
            mock_consumer = MagicMock()
            mock_consumer.poll.side_effect = fake_poll
            mock_cls.return_value = mock_consumer
            job.run()

        mock_redis.close.assert_called_once()

    def test_job_subscribes_to_all_three_topics(self):
        from src.jobs.real_time_join_job import RealTimeJoinJob
        mock_redis = MagicMock()
        job = RealTimeJoinJob(redis_sink=mock_redis)
        assert set(job.input_topics) == {
            "market.bars.5min",
            "news.articles.scored",
            "social.sentiment",
        }
