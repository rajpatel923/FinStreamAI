"""Knowledge graph — schema initialization, data import, and graph queries."""
from __future__ import annotations

from typing import Any

import structlog

from src.graph.neo4j_client import Neo4jClient

logger = structlog.get_logger(__name__)

_INIT_SCHEMA = """
CREATE CONSTRAINT company_symbol IF NOT EXISTS
    FOR (c:Company) REQUIRE c.symbol IS UNIQUE;
CREATE CONSTRAINT event_id IF NOT EXISTS
    FOR (e:Event) REQUIRE e.event_id IS UNIQUE;
CREATE CONSTRAINT article_id IF NOT EXISTS
    FOR (a:Article) REQUIRE a.article_id IS UNIQUE;
CREATE INDEX company_sector IF NOT EXISTS
    FOR (c:Company) ON (c.sector);
"""

_SEED_SECTORS = [
    "Technology", "Finance", "Healthcare", "Energy",
    "Consumer Discretionary", "Industrials", "Materials",
    "Real Estate", "Utilities", "Communication Services",
]


class KnowledgeGraph:
    """High-level graph operations over the Neo4j company/event knowledge graph."""

    def __init__(self, client: Neo4jClient) -> None:
        self._client = client

    # ------------------------------------------------------------------
    # Schema
    # ------------------------------------------------------------------
    def init_schema(self) -> None:
        """Create constraints, indexes, and seed sector nodes."""
        for statement in _INIT_SCHEMA.strip().split(";"):
            stmt = statement.strip()
            if stmt:
                try:
                    self._client.run(stmt)
                except Exception as exc:
                    logger.warning("Schema statement failed (may already exist)", error=str(exc))

        for sector in _SEED_SECTORS:
            self._client.run("MERGE (:Sector {name: $name})", {"name": sector})

        logger.info("Knowledge graph schema initialized")

    # ------------------------------------------------------------------
    # Data import
    # ------------------------------------------------------------------
    def import_company(self, symbol: str, name: str, sector: str) -> dict[str, Any]:
        result = self._client.run(
            """
            MERGE (c:Company {symbol: $symbol})
            SET c.name = $name, c.sector = $sector
            WITH c
            MATCH (s:Sector {name: $sector})
            MERGE (c)-[:PART_OF]->(s)
            RETURN c.symbol AS symbol, c.name AS name, c.sector AS sector
            """,
            {"symbol": symbol, "name": name, "sector": sector},
        )
        return result[0] if result else {}

    def link_event_to_company(self, event_id: str, symbol: str) -> None:
        self._client.run(
            """
            MATCH (c:Company {symbol: $symbol})
            MERGE (e:Event {event_id: $event_id})
            MERGE (c)-[:ANNOUNCED]->(e)
            """,
            {"event_id": event_id, "symbol": symbol},
        )

    def link_article_to_company(self, article_id: str, symbol: str) -> None:
        self._client.run(
            """
            MATCH (c:Company {symbol: $symbol})
            MERGE (a:Article {article_id: $article_id})
            MERGE (a)-[:MENTIONS]->(c)
            """,
            {"article_id": article_id, "symbol": symbol},
        )

    # ------------------------------------------------------------------
    # Queries
    # ------------------------------------------------------------------
    def get_company_network(self, symbol: str, depth: int = 2) -> list[dict[str, Any]]:
        """BFS subgraph up to *depth* hops from a Company node."""
        cypher = f"""
        MATCH path = (c:Company {{symbol: $symbol}})-[*1..{depth}]-(neighbor)
        RETURN DISTINCT neighbor
        LIMIT 50
        """
        return self._client.run(cypher, {"symbol": symbol})

    def find_affected_companies(self, event_id: str) -> list[dict[str, Any]]:
        """Traverse ANNOUNCED + PART_OF to find companies affected by an event."""
        return self._client.run(
            """
            MATCH (c:Company)-[:ANNOUNCED]->(e:Event {event_id: $event_id})
            OPTIONAL MATCH (peer:Company)-[:PART_OF]->(s:Sector)<-[:PART_OF]-(c)
            RETURN DISTINCT c.symbol AS symbol, c.name AS name, c.sector AS sector,
                            peer.symbol AS peer_symbol
            """,
            {"event_id": event_id},
        )

    def get_company(self, symbol: str) -> dict[str, Any] | None:
        result = self._client.run(
            "MATCH (c:Company {symbol: $symbol}) RETURN c.symbol AS symbol, c.name AS name, c.sector AS sector",
            {"symbol": symbol},
        )
        return result[0] if result else None

    def pagerank(self) -> list[dict[str, Any]]:
        """Run APOC PageRank on Company nodes (requires APOC plugin)."""
        try:
            return self._client.run(
                """
                CALL apoc.algo.pageRank(['Company'], ['PART_OF'], {iterations: 20, dampingFactor: 0.85})
                YIELD node, score
                RETURN node.symbol AS symbol, score
                ORDER BY score DESC
                LIMIT 20
                """
            )
        except Exception as exc:
            logger.warning("PageRank failed (APOC may not be available)", error=str(exc))
            return []

    def list_companies(self) -> list[dict[str, Any]]:
        return self._client.run(
            "MATCH (c:Company) RETURN c.symbol AS symbol, c.name AS name, c.sector AS sector ORDER BY c.symbol"
        )
