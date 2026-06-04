"""Tests for DataCatalog."""
from __future__ import annotations

from unittest.mock import MagicMock, patch, call
import pytest
from src.catalog.data_catalog import DataCatalog


@pytest.fixture
def mock_conn():
    conn = MagicMock()
    conn.closed = False
    cursor = MagicMock()
    cursor.__enter__ = MagicMock(return_value=cursor)
    cursor.__exit__ = MagicMock(return_value=False)
    cursor.fetchall.return_value = []
    conn.cursor.return_value = cursor
    return conn, cursor


class TestDataCatalog:
    def test_connect_creates_schema(self, mock_conn):
        conn, cursor = mock_conn
        with patch("psycopg2.connect", return_value=conn):
            catalog = DataCatalog("postgresql://test/test")
            catalog.connect()
        cursor.execute.assert_called()

    def test_upsert_partition(self, mock_conn):
        conn, cursor = mock_conn
        with patch("psycopg2.connect", return_value=conn):
            catalog = DataCatalog("postgresql://test/test")
            catalog.connect()
            catalog.upsert_partition("bronze", "market_tick", "2023-11-14", 1000)
        # upsert SQL was called
        calls = [str(c) for c in cursor.execute.call_args_list]
        assert any("INSERT" in c for c in calls)

    def test_get_partitions(self, mock_conn):
        conn, cursor = mock_conn
        cursor.fetchall.return_value = [
            {"layer": "bronze", "data_type": "market_tick", "partition_key": "2023-11-14",
             "record_count": 100, "schema_version": 1, "extra": {}, "updated_at": "now"}
        ]
        with patch("psycopg2.connect", return_value=conn):
            catalog = DataCatalog("postgresql://test/test")
            catalog.connect()
            results = catalog.get_partitions("bronze", "market_tick")
        # Returns list of dicts
        assert isinstance(results, list)

    def test_get_stats(self, mock_conn):
        conn, cursor = mock_conn
        cursor.fetchall.return_value = []
        with patch("psycopg2.connect", return_value=conn):
            catalog = DataCatalog("postgresql://test/test")
            catalog.connect()
            stats = catalog.get_stats()
        assert isinstance(stats, list)

    def test_list_tables(self, mock_conn):
        conn, cursor = mock_conn
        cursor.fetchall.return_value = []
        with patch("psycopg2.connect", return_value=conn):
            catalog = DataCatalog("postgresql://test/test")
            catalog.connect()
            tables = catalog.list_tables()
        assert isinstance(tables, list)

    def test_close(self, mock_conn):
        conn, cursor = mock_conn
        with patch("psycopg2.connect", return_value=conn):
            catalog = DataCatalog("postgresql://test/test")
            catalog.connect()
            catalog.close()
        conn.close.assert_called_once()
