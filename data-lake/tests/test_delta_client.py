"""Tests for DeltaClient."""
from __future__ import annotations

from unittest.mock import MagicMock, patch, call
import pandas as pd
import pytest


@pytest.fixture
def storage_options():
    return {
        "endpoint_url": "http://localhost:9000",
        "aws_access_key_id": "test",
        "aws_secret_access_key": "test",
        "aws_region": "us-east-1",
        "allow_http": "true",
    }


class TestDeltaClient:
    def test_write_calls_write_deltalake(self, storage_options):
        from src.lake.delta_client import DeltaClient

        client = DeltaClient(storage_options)
        df = pd.DataFrame([{"symbol": "AAPL", "price": 150.0}])

        with patch("src.lake.delta_client.write_deltalake") as mock_write:
            client.write("s3://bucket/test", df, mode="append", partition_by=["symbol"])
            mock_write.assert_called_once_with(
                "s3://bucket/test",
                df,
                mode="append",
                partition_by=["symbol"],
                schema_mode="merge",
                storage_options=storage_options,
            )

    def test_write_empty_df_skipped(self, storage_options):
        from src.lake.delta_client import DeltaClient

        client = DeltaClient(storage_options)
        df = pd.DataFrame()

        with patch("src.lake.delta_client.write_deltalake") as mock_write:
            client.write("s3://bucket/test", df)
            mock_write.assert_not_called()

    def test_read_returns_dataframe(self, storage_options):
        from src.lake.delta_client import DeltaClient

        client = DeltaClient(storage_options)
        expected_df = pd.DataFrame([{"symbol": "AAPL", "price": 150.0}])

        mock_dt = MagicMock()
        mock_dt.to_pandas.return_value = expected_df

        with patch("src.lake.delta_client.DeltaTable", return_value=mock_dt) as mock_cls:
            result = client.read("s3://bucket/test", version=None)
            mock_cls.assert_called_once_with("s3://bucket/test", storage_options=storage_options, version=None)
            assert result.equals(expected_df)

    def test_read_with_version(self, storage_options):
        from src.lake.delta_client import DeltaClient

        client = DeltaClient(storage_options)
        mock_dt = MagicMock()
        mock_dt.to_pandas.return_value = pd.DataFrame()

        with patch("src.lake.delta_client.DeltaTable", return_value=mock_dt):
            client.read("s3://bucket/test", version=5)
            mock_dt.to_pandas.assert_called_once()

    def test_read_filtered(self, storage_options):
        from src.lake.delta_client import DeltaClient

        client = DeltaClient(storage_options)
        mock_dt = MagicMock()
        mock_dt.to_pandas.return_value = pd.DataFrame([{"symbol": "AAPL"}])

        with patch("src.lake.delta_client.DeltaTable", return_value=mock_dt):
            filters = [("symbol", "=", "AAPL")]
            result = client.read_filtered("s3://bucket/test", filters=filters)
            mock_dt.to_pandas.assert_called_once_with(filters=filters)

    def test_vacuum(self, storage_options):
        from src.lake.delta_client import DeltaClient

        client = DeltaClient(storage_options)
        mock_dt = MagicMock()

        with patch("src.lake.delta_client.DeltaTable", return_value=mock_dt):
            client.vacuum("s3://bucket/test", retention_hours=24)
            mock_dt.vacuum.assert_called_once_with(
                retention_hours=24,
                dry_run=False,
                enforce_retention_duration=False,
            )

    def test_get_history(self, storage_options):
        from src.lake.delta_client import DeltaClient

        client = DeltaClient(storage_options)
        mock_dt = MagicMock()
        mock_dt.history.return_value = [{"version": 0, "operation": "WRITE"}]

        with patch("src.lake.delta_client.DeltaTable", return_value=mock_dt):
            history = client.get_history("s3://bucket/test", limit=5)
            assert len(history) == 1
            mock_dt.history.assert_called_once_with(limit=5)

    def test_table_exists_true(self, storage_options):
        from src.lake.delta_client import DeltaClient

        client = DeltaClient(storage_options)

        with patch("src.lake.delta_client.DeltaTable") as mock_cls:
            mock_cls.return_value = MagicMock()
            assert client.table_exists("s3://bucket/test") is True

    def test_table_exists_false(self, storage_options):
        from src.lake.delta_client import DeltaClient

        client = DeltaClient(storage_options)

        with patch("src.lake.delta_client.DeltaTable", side_effect=Exception("not found")):
            assert client.table_exists("s3://bucket/test") is False
