"""Data Lake FastAPI application."""
from __future__ import annotations

import threading
import time
from contextlib import asynccontextmanager

import logging
import os

import structlog
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from prometheus_client import make_asgi_app

from src.config import settings
from src.middleware.auth import JWTAuthMiddleware
from src.cache.redis_cache import RedisCache
from src.connectors.kafka_sink import KafkaSinkConsumer
from src.graph.knowledge_graph import KnowledgeGraph
from src.graph.neo4j_client import Neo4jClient
from src.lake.bronze_layer import BronzeLayer
from src.lake.delta_client import DeltaClient
from src.lake.gold_layer import GoldLayer
from src.lake.silver_layer import SilverLayer
from src.quality.quarantine import Quarantine
from src.query.unified_query import UnifiedQuery

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=getattr(logging, LOG_LEVEL, logging.INFO))
structlog.configure(
    wrapper_class=structlog.make_filtering_bound_logger(
        getattr(logging, LOG_LEVEL, logging.INFO)
    )
)

logger = structlog.get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting FinStreamAI Data Lake", port=settings.DATA_LAKE_METRICS_PORT)

    # Delta client
    delta_client = DeltaClient(settings.delta_storage_options)

    # Quality / Quarantine
    quarantine = Quarantine(
        delta_client=delta_client,
        quarantine_path=f"{settings.bronze_path}/quarantine",
    )

    # Bronze / Silver / Gold layers
    bronze = BronzeLayer(delta_client, settings.bronze_path, quarantine)
    silver = SilverLayer(delta_client, settings.bronze_path, settings.silver_path)
    gold = GoldLayer(delta_client, settings.silver_path, settings.gold_path)

    # Neo4j
    neo4j_client = Neo4jClient(settings.NEO4J_URI, settings.NEO4J_USER, settings.NEO4J_PASSWORD)
    knowledge_graph: KnowledgeGraph | None = None
    try:
        neo4j_client.connect()
        knowledge_graph = KnowledgeGraph(neo4j_client)
        knowledge_graph.init_schema()
    except Exception as exc:
        logger.warning("Neo4j unavailable at startup — graph features disabled", error=str(exc))
        neo4j_client = None

    # Redis cache
    cache = RedisCache(settings.REDIS_HOST, settings.REDIS_PORT, settings.REDIS_PASSWORD or None)
    try:
        cache.connect()
        cache.warm_prices(settings.watched_symbols_list, lambda s: None)
    except Exception as exc:
        logger.warning("Redis unavailable at startup — cache disabled", error=str(exc))
        cache = None

    # Unified query
    timescale_dsn = settings.timescaledb_sync_url.replace("+asyncpg", "").replace("postgresql+asyncpg", "postgresql")
    unified_query = UnifiedQuery(
        timescale_dsn=timescale_dsn,
        neo4j_client=neo4j_client,
        redis_cache=cache,
    )

    # Kafka sink consumer (daemon thread)
    consumer_cfg = settings.consumer_config("data-lake-sink")
    kafka_sink = KafkaSinkConsumer(consumer_cfg, bronze)
    kafka_thread = threading.Thread(target=kafka_sink.run, name="kafka-sink", daemon=True)
    kafka_thread.start()
    logger.info("Kafka sink thread started")

    # Expose state on app
    app.state.delta_client = delta_client
    app.state.quarantine = quarantine
    app.state.bronze_layer = bronze
    app.state.silver_layer = silver
    app.state.gold_layer = gold
    app.state.neo4j_client = neo4j_client
    app.state.knowledge_graph = knowledge_graph
    app.state.cache = cache
    app.state.unified_query = unified_query
    app.state.timescale_dsn = timescale_dsn
    app.state.kafka_sink = kafka_sink

    logger.info("Data Lake service ready")
    yield

    # Shutdown
    kafka_sink.shutdown()
    if neo4j_client:
        neo4j_client.close()
    if cache:
        cache.close()
    logger.info("Data Lake service stopped")


app = FastAPI(
    title="FinStreamAI — Data Lake",
    version="0.1.0",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan,
)

app.add_middleware(JWTAuthMiddleware, secret_key=settings.JWT_SECRET_KEY)
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ORIGINS or ["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def add_process_time_header(request: Request, call_next):
    start = time.perf_counter()
    response = await call_next(request)
    response.headers["X-Process-Time"] = f"{(time.perf_counter() - start) * 1000:.2f}ms"
    return response


# Prometheus
metrics_app = make_asgi_app()
app.mount("/metrics", metrics_app)

# Routes
from src.api import health, lake_router, graph_router, query_router, cache_router  # noqa: E402

app.include_router(health.router, prefix=settings.API_V1_PREFIX)
app.include_router(lake_router.router, prefix=settings.API_V1_PREFIX)
app.include_router(graph_router.router, prefix=settings.API_V1_PREFIX)
app.include_router(query_router.router, prefix=settings.API_V1_PREFIX)
app.include_router(cache_router.router, prefix=settings.API_V1_PREFIX)

# Root shortcuts
app.add_api_route("/health", health.health, include_in_schema=False)
app.add_api_route("/ready", health.ready, include_in_schema=False)
app.add_api_route("/live", health.live, include_in_schema=False)


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    logger.error("Unhandled exception", path=request.url.path, error=str(exc))
    return JSONResponse({"detail": "Internal server error"}, status_code=500)
