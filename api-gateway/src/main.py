"""API Gateway — FastAPI application entry point."""
from __future__ import annotations

import asyncio
import time
from contextlib import asynccontextmanager

import structlog
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from prometheus_client import make_asgi_app
from slowapi import _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded

from src.core.config import settings
from src.core.database import close_db, init_db
from src.middleware.logging import CorrelationIdMiddleware
from src.middleware.rate_limit import limiter
from src.services.ws_manager import ConnectionManager, KafkaBridgeConsumer

logger = structlog.get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting FinStreamAI API Gateway", port=settings.GATEWAY_PORT)
    await init_db()

    # ── WebSocket manager ────────────────────────────────────────────
    manager = ConnectionManager()
    app.state.ws_manager = manager

    from src.api.v1 import websocket as ws_module
    ws_module.set_manager(manager)

    # ── Kafka bridge (connect to WS manager) ────────────────────────
    bridge = KafkaBridgeConsumer(
        manager=manager,
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        redis_url=settings.redis_url,
    )
    loop = asyncio.get_event_loop()
    bridge.start(loop)
    app.state.kafka_bridge = bridge

    # ── OpenTelemetry ────────────────────────────────────────────────
    if settings.OTEL_ENABLED:
        try:
            from opentelemetry import trace
            from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
            from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
            from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor
            from opentelemetry.sdk.resources import SERVICE_NAME, Resource
            from opentelemetry.sdk.trace import TracerProvider
            from opentelemetry.sdk.trace.export import BatchSpanProcessor

            resource = Resource(attributes={SERVICE_NAME: settings.OTEL_SERVICE_NAME})
            provider = TracerProvider(resource=resource)
            provider.add_span_processor(
                BatchSpanProcessor(OTLPSpanExporter(endpoint=settings.OTEL_EXPORTER_OTLP_ENDPOINT))
            )
            trace.set_tracer_provider(provider)
            FastAPIInstrumentor.instrument_app(app)
            logger.info("OpenTelemetry enabled")
        except ImportError:
            logger.warning("OpenTelemetry packages not installed — tracing disabled")

    logger.info("API Gateway ready")
    yield

    bridge.stop()
    await close_db()
    logger.info("API Gateway stopped")


app = FastAPI(
    title="FinStreamAI — API Gateway",
    version="0.1.0",
    docs_url="/docs" if settings.DEBUG else None,
    redoc_url="/redoc" if settings.DEBUG else None,
    openapi_url="/openapi.json" if settings.DEBUG else None,
    lifespan=lifespan,
)

# ─── Rate limiter ────────────────────────────────────────────────────────────
limiter._storage_uri = settings.redis_url  # type: ignore[attr-defined]
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# ─── Middleware ───────────────────────────────────────────────────────────────
app.add_middleware(CorrelationIdMiddleware)
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


# ─── Prometheus ───────────────────────────────────────────────────────────────
metrics_app = make_asgi_app()
app.mount("/metrics", metrics_app)

# ─── Routes ───────────────────────────────────────────────────────────────────
from src.api.v1 import auth, users, query, analytics, alerts, export, websocket, health  # noqa: E402
from src.api.v1 import watchlist, preferences, internal  # noqa: E402

app.include_router(health.router, prefix=settings.API_V1_PREFIX)
app.include_router(auth.router, prefix=settings.API_V1_PREFIX)
app.include_router(users.router, prefix=settings.API_V1_PREFIX)
app.include_router(query.router, prefix=settings.API_V1_PREFIX)
app.include_router(analytics.router, prefix=settings.API_V1_PREFIX)
app.include_router(alerts.router, prefix=settings.API_V1_PREFIX)
app.include_router(export.router, prefix=settings.API_V1_PREFIX)
app.include_router(watchlist.router, prefix=settings.API_V1_PREFIX)
app.include_router(preferences.router, prefix=settings.API_V1_PREFIX)
app.include_router(internal.router, prefix=settings.API_V1_PREFIX)
app.include_router(websocket.router)  # no prefix — path is /ws/stream

# Root shortcuts
app.add_api_route("/health", health.health, include_in_schema=False)


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    logger.error("Unhandled exception", path=request.url.path, error=str(exc))
    return JSONResponse({"detail": "Internal server error"}, status_code=500)
