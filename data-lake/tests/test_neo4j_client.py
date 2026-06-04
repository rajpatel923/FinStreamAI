"""Tests for Neo4jClient."""
from __future__ import annotations

from unittest.mock import MagicMock, patch
import pytest
from src.graph.neo4j_client import Neo4jClient


@pytest.fixture
def client():
    c = Neo4jClient("bolt://localhost:7687", "neo4j", "test")
    return c


class TestNeo4jClient:
    def test_connect_verifies_connectivity(self, client):
        mock_driver = MagicMock()
        with patch("src.graph.neo4j_client.GraphDatabase.driver", return_value=mock_driver):
            client.connect()
            mock_driver.verify_connectivity.assert_called_once()

    def test_close_closes_driver(self, client, mock_neo4j_driver):
        client._driver = mock_neo4j_driver
        client.close()
        mock_neo4j_driver.close.assert_called_once()
        assert client._driver is None

    def test_close_without_connect(self, client):
        # Should not raise
        client.close()

    def test_run_returns_list(self, client, mock_neo4j_driver):
        session = MagicMock()
        record = MagicMock()
        record.items.return_value = [("n", 1)]
        # Make dict(record) work by making record a dict-like
        mock_neo4j_driver.session.return_value.__enter__ = MagicMock(return_value=session)
        mock_neo4j_driver.session.return_value.__exit__ = MagicMock(return_value=False)
        session.run.return_value = []
        client._driver = mock_neo4j_driver
        result = client.run("RETURN 1")
        assert isinstance(result, list)

    def test_run_retries_on_failure(self, client, mock_neo4j_driver):
        call_count = [0]
        session = MagicMock()

        def fake_enter(*a):
            call_count[0] += 1
            if call_count[0] < 2:
                raise Exception("transient error")
            return session

        mock_neo4j_driver.session.return_value.__enter__ = fake_enter
        mock_neo4j_driver.session.return_value.__exit__ = MagicMock(return_value=False)
        session.run.return_value = []
        client._driver = mock_neo4j_driver

        with patch("src.graph.neo4j_client._RETRY_DELAY_S", 0):
            result = client.run("RETURN 1", retries=3)
        assert isinstance(result, list)

    def test_run_raises_after_max_retries(self, client, mock_neo4j_driver):
        mock_neo4j_driver.session.return_value.__enter__ = MagicMock(side_effect=Exception("persistent error"))
        mock_neo4j_driver.session.return_value.__exit__ = MagicMock(return_value=False)
        client._driver = mock_neo4j_driver

        with patch("src.graph.neo4j_client._RETRY_DELAY_S", 0):
            with pytest.raises(RuntimeError, match="failed after"):
                client.run("RETURN 1", retries=2)

    def test_run_write(self, client, mock_neo4j_driver):
        session = MagicMock()
        session.execute_write.return_value = []
        mock_neo4j_driver.session.return_value.__enter__ = MagicMock(return_value=session)
        mock_neo4j_driver.session.return_value.__exit__ = MagicMock(return_value=False)
        client._driver = mock_neo4j_driver
        result = client.run_write("CREATE (:Test)")
        assert isinstance(result, list)
