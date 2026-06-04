"""Neo4j bolt driver wrapper with connection pooling and retry."""
from __future__ import annotations

import time
from typing import Any

import structlog
from neo4j import GraphDatabase, Driver, Session

logger = structlog.get_logger(__name__)

_MAX_RETRIES = 3
_RETRY_DELAY_S = 2.0


class Neo4jClient:
    """Thin wrapper around the Neo4j Python driver."""

    def __init__(self, uri: str, user: str, password: str) -> None:
        self._uri = uri
        self._auth = (user, password)
        self._driver: Driver | None = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    def connect(self) -> None:
        self._driver = GraphDatabase.driver(self._uri, auth=self._auth)
        self._driver.verify_connectivity()
        logger.info("Neo4j connected", uri=self._uri)

    def close(self) -> None:
        if self._driver:
            self._driver.close()
            self._driver = None

    # ------------------------------------------------------------------
    # Query helpers
    # ------------------------------------------------------------------
    def run(
        self,
        cypher: str,
        parameters: dict[str, Any] | None = None,
        *,
        retries: int = _MAX_RETRIES,
    ) -> list[dict[str, Any]]:
        """Run a Cypher query and return a list of record dicts."""
        last_exc: Exception | None = None
        for attempt in range(retries):
            try:
                with self._driver.session() as session:
                    result = session.run(cypher, parameters or {})
                    return [dict(record) for record in result]
            except Exception as exc:
                last_exc = exc
                logger.warning("Neo4j query failed", attempt=attempt + 1, error=str(exc))
                if attempt < retries - 1:
                    time.sleep(_RETRY_DELAY_S * (attempt + 1))
        raise RuntimeError(f"Neo4j query failed after {retries} attempts") from last_exc

    def run_write(
        self,
        cypher: str,
        parameters: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        """Run a write transaction."""
        with self._driver.session() as session:
            result = session.execute_write(
                lambda tx: list(tx.run(cypher, parameters or {}))
            )
            return [dict(r) for r in result]
