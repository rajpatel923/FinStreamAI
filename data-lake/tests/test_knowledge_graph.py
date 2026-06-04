"""Tests for KnowledgeGraph."""
from __future__ import annotations

from unittest.mock import MagicMock, call
import pytest
from src.graph.knowledge_graph import KnowledgeGraph


class TestKnowledgeGraph:
    def test_init_schema_runs_cypher(self, knowledge_graph, neo4j_client):
        neo4j_client.run = MagicMock(return_value=[])
        knowledge_graph.init_schema()
        assert neo4j_client.run.call_count > 0

    def test_import_company(self, knowledge_graph, neo4j_client):
        neo4j_client.run = MagicMock(return_value=[{"symbol": "AAPL", "name": "Apple", "sector": "Technology"}])
        result = knowledge_graph.import_company("AAPL", "Apple", "Technology")
        assert result["symbol"] == "AAPL"
        neo4j_client.run.assert_called()

    def test_import_company_empty_result(self, knowledge_graph, neo4j_client):
        neo4j_client.run = MagicMock(return_value=[])
        result = knowledge_graph.import_company("XXX", "Unknown", "Other")
        assert result == {}

    def test_link_event_to_company(self, knowledge_graph, neo4j_client):
        neo4j_client.run = MagicMock(return_value=[])
        knowledge_graph.link_event_to_company("evt-1", "AAPL")
        neo4j_client.run.assert_called_once()

    def test_link_article_to_company(self, knowledge_graph, neo4j_client):
        neo4j_client.run = MagicMock(return_value=[])
        knowledge_graph.link_article_to_company("art-1", "MSFT")
        neo4j_client.run.assert_called_once()

    def test_get_company_network(self, knowledge_graph, neo4j_client):
        neo4j_client.run = MagicMock(return_value=[{"neighbor": "some_node"}])
        result = knowledge_graph.get_company_network("AAPL", depth=1)
        assert isinstance(result, list)

    def test_find_affected_companies(self, knowledge_graph, neo4j_client):
        neo4j_client.run = MagicMock(return_value=[{"symbol": "AAPL", "peer_symbol": "MSFT"}])
        result = knowledge_graph.find_affected_companies("evt-1")
        assert len(result) == 1

    def test_get_company_found(self, knowledge_graph, neo4j_client):
        neo4j_client.run = MagicMock(return_value=[{"symbol": "AAPL", "name": "Apple", "sector": "Technology"}])
        result = knowledge_graph.get_company("AAPL")
        assert result is not None
        assert result["symbol"] == "AAPL"

    def test_get_company_not_found(self, knowledge_graph, neo4j_client):
        neo4j_client.run = MagicMock(return_value=[])
        result = knowledge_graph.get_company("ZZZZ")
        assert result is None

    def test_pagerank_with_apoc(self, knowledge_graph, neo4j_client):
        neo4j_client.run = MagicMock(return_value=[{"symbol": "AAPL", "score": 0.9}])
        result = knowledge_graph.pagerank()
        assert isinstance(result, list)

    def test_pagerank_apoc_unavailable(self, knowledge_graph, neo4j_client):
        neo4j_client.run = MagicMock(side_effect=Exception("APOC not installed"))
        result = knowledge_graph.pagerank()
        assert result == []

    def test_list_companies(self, knowledge_graph, neo4j_client):
        neo4j_client.run = MagicMock(return_value=[
            {"symbol": "AAPL", "name": "Apple", "sector": "Technology"},
        ])
        result = knowledge_graph.list_companies()
        assert len(result) == 1
