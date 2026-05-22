"""Tests for TimescaleDBSink."""
import time
import uuid
from unittest.mock import MagicMock, call, patch

import psycopg2
import pytest


def make_bar(symbol="AAPL"):
    return {
        "bar_id": str(uuid.uuid4()),
        "symbol": symbol,
        "timeframe": "1min",
        "timestamp_ms": int(time.time() * 1000),
        "open": 100.0,
        "high": 102.0,
        "low": 99.0,
        "close": 101.0,
        "volume": 5000,
        "vwap": 100.5,
        "trade_count": 100,
        "source": "alpha_vantage",
    }


def make_indicator(symbol="AAPL", name="RSI14"):
    return {
        "indicator_id": str(uuid.uuid4()),
        "symbol": symbol,
        "timestamp_ms": int(time.time() * 1000),
        "indicator_name": name,
        "timeframe": "1min",
        "value": 55.0,
        "signal_value": None,
        "upper_band": None,
        "lower_band": None,
        "metadata": {"period": 14},
    }


def make_signal(symbol="AAPL"):
    return {
        "signal_id": str(uuid.uuid4()),
        "symbol": symbol,
        "timestamp_ms": int(time.time() * 1000),
        "signal_type": "RSI_OVERBOUGHT",
        "direction": "bearish",
        "strength": 0.8,
        "indicator_values": {"RSI14": 75.0},
        "source": "signal-generation",
    }


@pytest.fixture
def mock_psycopg2_conn():
    with patch("src.sinks.timescaledb_sink.psycopg2.connect") as mock_connect:
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_conn.closed = False
        mock_connect.return_value = mock_conn
        yield mock_conn, mock_cursor, mock_connect


class TestTimescaleDBSinkLazyConnection:
    def test_no_connection_on_init(self):
        from src.sinks.timescaledb_sink import TimescaleDBSink
        sink = TimescaleDBSink(dsn="postgresql://localhost/test")
        assert sink._conn is None

    def test_connection_created_on_first_get(self, mock_psycopg2_conn):
        mock_conn, mock_cursor, mock_connect = mock_psycopg2_conn
        from src.sinks.timescaledb_sink import TimescaleDBSink
        sink = TimescaleDBSink(dsn="postgresql://localhost/test")
        conn = sink._get_conn()
        mock_connect.assert_called_once()
        assert conn is mock_conn

    def test_connection_cached(self, mock_psycopg2_conn):
        mock_conn, mock_cursor, mock_connect = mock_psycopg2_conn
        from src.sinks.timescaledb_sink import TimescaleDBSink
        sink = TimescaleDBSink(dsn="postgresql://localhost/test")
        c1 = sink._get_conn()
        c2 = sink._get_conn()
        assert c1 is c2
        mock_connect.assert_called_once()

    def test_reconnects_when_connection_closed(self, mock_psycopg2_conn):
        mock_conn, mock_cursor, mock_connect = mock_psycopg2_conn
        from src.sinks.timescaledb_sink import TimescaleDBSink
        sink = TimescaleDBSink(dsn="postgresql://localhost/test")
        mock_conn.closed = True
        sink._conn = mock_conn  # pre-inject stale closed conn
        sink._get_conn()
        assert mock_connect.call_count == 1


class TestTimescaleDBSinkWriteBar:
    def test_write_bar_appends_to_buffer(self):
        from src.sinks.timescaledb_sink import TimescaleDBSink
        sink = TimescaleDBSink(dsn="postgresql://localhost/test", batch_size=100)
        sink.write_market_bar(make_bar())
        assert len(sink._bar_buffer) == 1

    def test_write_bar_auto_flushes_at_batch_size(self, mock_psycopg2_conn):
        from src.sinks.timescaledb_sink import TimescaleDBSink
        sink = TimescaleDBSink(dsn="postgresql://localhost/test", batch_size=3)
        sink.write_market_bar(make_bar())
        sink.write_market_bar(make_bar())
        assert len(sink._bar_buffer) == 2
        sink.write_market_bar(make_bar())  # triggers flush
        assert len(sink._bar_buffer) == 0

    def test_flush_bars_executes_insert(self, mock_psycopg2_conn):
        mock_conn, mock_cursor, mock_connect = mock_psycopg2_conn
        from src.sinks.timescaledb_sink import TimescaleDBSink
        sink = TimescaleDBSink(dsn="postgresql://localhost/test", batch_size=100)
        sink.write_market_bar(make_bar("AAPL"))
        sink._flush_bars()
        mock_cursor.execute.assert_called_once()
        sql = mock_cursor.execute.call_args[0][0]
        assert "INSERT INTO market_bars" in sql
        assert "ON CONFLICT" in sql

    def test_flush_bars_commits_transaction(self, mock_psycopg2_conn):
        mock_conn, mock_cursor, mock_connect = mock_psycopg2_conn
        from src.sinks.timescaledb_sink import TimescaleDBSink
        sink = TimescaleDBSink(dsn="postgresql://localhost/test")
        sink.write_market_bar(make_bar())
        sink._flush_bars()
        mock_conn.commit.assert_called_once()

    def test_flush_bars_noop_when_buffer_empty(self, mock_psycopg2_conn):
        mock_conn, mock_cursor, mock_connect = mock_psycopg2_conn
        from src.sinks.timescaledb_sink import TimescaleDBSink
        sink = TimescaleDBSink(dsn="postgresql://localhost/test")
        sink._flush_bars()
        mock_connect.assert_not_called()

    def test_flush_bars_increments_metric(self, mock_psycopg2_conn):
        from unittest.mock import patch as mpatch
        from src.sinks.timescaledb_sink import TimescaleDBSink
        sink = TimescaleDBSink(dsn="postgresql://localhost/test")
        sink.write_market_bar(make_bar())
        with mpatch("src.sinks.timescaledb_sink.timescaledb_writes_total") as mock_metric:
            mock_labels = MagicMock()
            mock_metric.labels.return_value = mock_labels
            sink._flush_bars()
            mock_metric.labels.assert_called_with(table="market_bars")
            mock_labels.inc.assert_called_once_with(1)


class TestTimescaleDBSinkWriteIndicator:
    def test_write_indicator_appends_to_buffer(self):
        from src.sinks.timescaledb_sink import TimescaleDBSink
        sink = TimescaleDBSink(dsn="postgresql://localhost/test", batch_size=100)
        sink.write_technical_indicator(make_indicator())
        assert len(sink._indicator_buffer) == 1

    def test_write_indicator_auto_flushes_at_batch_size(self, mock_psycopg2_conn):
        from src.sinks.timescaledb_sink import TimescaleDBSink
        sink = TimescaleDBSink(dsn="postgresql://localhost/test", batch_size=2)
        sink.write_technical_indicator(make_indicator())
        assert len(sink._indicator_buffer) == 1
        sink.write_technical_indicator(make_indicator())
        assert len(sink._indicator_buffer) == 0

    def test_flush_indicators_executes_insert(self, mock_psycopg2_conn):
        mock_conn, mock_cursor, mock_connect = mock_psycopg2_conn
        from src.sinks.timescaledb_sink import TimescaleDBSink
        sink = TimescaleDBSink(dsn="postgresql://localhost/test")
        sink.write_technical_indicator(make_indicator())
        sink._flush_indicators()
        sql = mock_cursor.execute.call_args[0][0]
        assert "INSERT INTO technical_indicators" in sql

    def test_flush_indicators_serializes_metadata_as_json(self, mock_psycopg2_conn):
        mock_conn, mock_cursor, mock_connect = mock_psycopg2_conn
        from src.sinks.timescaledb_sink import TimescaleDBSink
        sink = TimescaleDBSink(dsn="postgresql://localhost/test")
        ind = make_indicator()
        ind["metadata"] = {"period": 14, "signal": "EMA"}
        sink.write_technical_indicator(ind)
        sink._flush_indicators()
        params = mock_cursor.execute.call_args[0][1]
        # metadata is the last param
        import json
        metadata_json = params[-1]
        parsed = json.loads(metadata_json)
        assert parsed["period"] == 14


class TestTimescaleDBSinkWriteSignal:
    def test_write_signal_appends_to_buffer(self):
        from src.sinks.timescaledb_sink import TimescaleDBSink
        sink = TimescaleDBSink(dsn="postgresql://localhost/test", batch_size=100)
        sink.write_trading_signal(make_signal())
        assert len(sink._signal_buffer) == 1

    def test_write_signal_auto_flushes_at_batch_size(self, mock_psycopg2_conn):
        from src.sinks.timescaledb_sink import TimescaleDBSink
        sink = TimescaleDBSink(dsn="postgresql://localhost/test", batch_size=2)
        sink.write_trading_signal(make_signal())
        assert len(sink._signal_buffer) == 1
        sink.write_trading_signal(make_signal())
        assert len(sink._signal_buffer) == 0

    def test_flush_signals_executes_insert(self, mock_psycopg2_conn):
        mock_conn, mock_cursor, mock_connect = mock_psycopg2_conn
        from src.sinks.timescaledb_sink import TimescaleDBSink
        sink = TimescaleDBSink(dsn="postgresql://localhost/test")
        sink.write_trading_signal(make_signal())
        sink._flush_signals()
        sql = mock_cursor.execute.call_args[0][0]
        assert "INSERT INTO trading_signals" in sql
        assert "ON CONFLICT" in sql

    def test_flush_signals_serializes_indicator_values(self, mock_psycopg2_conn):
        mock_conn, mock_cursor, mock_connect = mock_psycopg2_conn
        from src.sinks.timescaledb_sink import TimescaleDBSink
        sink = TimescaleDBSink(dsn="postgresql://localhost/test")
        sig = make_signal()
        sig["indicator_values"] = {"RSI14": 75.0, "MACD": 0.5}
        sink.write_trading_signal(sig)
        sink._flush_signals()
        params = mock_cursor.execute.call_args[0][1]
        import json
        # indicator_values is the 6th param (index 5)
        parsed = json.loads(params[5])
        assert parsed["RSI14"] == 75.0


class TestTimescaleDBSinkFlushClose:
    def test_flush_calls_all_flush_methods(self, mock_psycopg2_conn):
        from src.sinks.timescaledb_sink import TimescaleDBSink
        sink = TimescaleDBSink(dsn="postgresql://localhost/test")
        sink._flush_bars = MagicMock()
        sink._flush_indicators = MagicMock()
        sink._flush_signals = MagicMock()
        sink.flush()
        sink._flush_bars.assert_called_once()
        sink._flush_indicators.assert_called_once()
        sink._flush_signals.assert_called_once()

    def test_close_flushes_and_closes_connection(self, mock_psycopg2_conn):
        mock_conn, mock_cursor, mock_connect = mock_psycopg2_conn
        from src.sinks.timescaledb_sink import TimescaleDBSink
        sink = TimescaleDBSink(dsn="postgresql://localhost/test")
        sink._conn = mock_conn
        sink.close()
        mock_conn.close.assert_called_once()
        assert sink._conn is None

    def test_connection_failure_clears_conn_reference(self, mock_psycopg2_conn):
        mock_conn, mock_cursor, mock_connect = mock_psycopg2_conn
        mock_conn.cursor.return_value.__enter__.side_effect = psycopg2.OperationalError("lost")
        from src.sinks.timescaledb_sink import TimescaleDBSink
        sink = TimescaleDBSink(dsn="postgresql://localhost/test")
        sink._conn = mock_conn
        sink._bar_buffer.append(make_bar())
        with pytest.raises(Exception):
            sink._flush_bars()
        assert sink._conn is None
