"""Shared fixtures for data-lake tests."""
from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

# Ensure src is importable
_ROOT = Path(__file__).resolve().parents[2]
_SRC = Path(__file__).resolve().parents[1] / "src"
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))
if str(_SRC.parent) not in sys.path:
    sys.path.insert(0, str(_SRC.parent))


# ---------------------------------------------------------------------------
# DeltaClient mock
# ---------------------------------------------------------------------------
@pytest.fixture
def mock_delta_client():
    client = MagicMock()
    client.table_exists.return_value = True
    client.read.return_value = pd.DataFrame()
    client.write.return_value = None
    client.vacuum.return_value = None
    client.get_history.return_value = []
    return client


# ---------------------------------------------------------------------------
# Quarantine (real, in-memory)
# ---------------------------------------------------------------------------
@pytest.fixture
def quarantine(mock_delta_client):
    from src.quality.quarantine import Quarantine

    return Quarantine(delta_client=None, quarantine_path=None)


# ---------------------------------------------------------------------------
# Bronze layer
# ---------------------------------------------------------------------------
@pytest.fixture
def bronze_layer(mock_delta_client, quarantine):
    from src.lake.bronze_layer import BronzeLayer

    return BronzeLayer(
        delta_client=mock_delta_client,
        base_path="s3://test-bucket/bronze",
        quarantine=quarantine,
    )


# ---------------------------------------------------------------------------
# Silver layer
# ---------------------------------------------------------------------------
@pytest.fixture
def silver_layer(mock_delta_client):
    from src.lake.silver_layer import SilverLayer

    return SilverLayer(
        delta_client=mock_delta_client,
        bronze_base="s3://test-bucket/bronze",
        silver_base="s3://test-bucket/silver",
    )


# ---------------------------------------------------------------------------
# Gold layer
# ---------------------------------------------------------------------------
@pytest.fixture
def gold_layer(mock_delta_client):
    from src.lake.gold_layer import GoldLayer

    return GoldLayer(
        delta_client=mock_delta_client,
        silver_base="s3://test-bucket/silver",
        gold_base="s3://test-bucket/gold",
    )


# ---------------------------------------------------------------------------
# Neo4j driver mock
# ---------------------------------------------------------------------------
@pytest.fixture
def mock_neo4j_driver():
    driver = MagicMock()
    session = MagicMock()
    driver.session.return_value.__enter__ = MagicMock(return_value=session)
    driver.session.return_value.__exit__ = MagicMock(return_value=False)
    session.run.return_value = []
    session.execute_write.return_value = []
    driver.verify_connectivity.return_value = None
    return driver


@pytest.fixture
def neo4j_client(mock_neo4j_driver):
    from src.graph.neo4j_client import Neo4jClient

    client = Neo4jClient("bolt://localhost:7687", "neo4j", "test")
    client._driver = mock_neo4j_driver
    return client


@pytest.fixture
def knowledge_graph(neo4j_client):
    from src.graph.knowledge_graph import KnowledgeGraph

    return KnowledgeGraph(neo4j_client)


# ---------------------------------------------------------------------------
# Redis (fakeredis)
# ---------------------------------------------------------------------------
@pytest.fixture
def fake_redis():
    try:
        import fakeredis

        r = fakeredis.FakeRedis(decode_responses=True)
        return r
    except ImportError:
        return MagicMock()


@pytest.fixture
def redis_cache(fake_redis):
    from src.cache.redis_cache import RedisCache

    cache = RedisCache("localhost", 6379, None)
    cache._client = fake_redis
    return cache


# ---------------------------------------------------------------------------
# Kafka consumer mock
# ---------------------------------------------------------------------------
@pytest.fixture
def mock_kafka_consumer():
    consumer = MagicMock()
    consumer.poll.return_value = None
    consumer.subscribe.return_value = None
    consumer.commit.return_value = None
    consumer.close.return_value = None
    return consumer


# ---------------------------------------------------------------------------
# Data catalog mock
# ---------------------------------------------------------------------------
@pytest.fixture
def mock_data_catalog():
    catalog = MagicMock()
    catalog.get_stats.return_value = []
    catalog.get_partitions.return_value = []
    catalog.upsert_partition.return_value = None
    catalog.list_tables.return_value = []
    return catalog


# ---------------------------------------------------------------------------
# Unified query
# ---------------------------------------------------------------------------
@pytest.fixture
def unified_query(neo4j_client, redis_cache):
    from src.query.unified_query import UnifiedQuery

    return UnifiedQuery(
        timescale_dsn=None,
        postgres_dsn=None,
        neo4j_client=neo4j_client,
        redis_cache=redis_cache,
    )
