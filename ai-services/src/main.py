from __future__ import annotations

import threading
import time
from contextlib import asynccontextmanager

import structlog
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from prometheus_client import make_asgi_app

from src.config import settings
from src.middleware.auth import JWTAuthMiddleware
from src.consumers.events_consumer import EventsConsumer
from src.consumers.sentiment_consumer import SentimentConsumer
from src.embeddings.chroma_service import ChromaService
from src.events.claude_extractor import ClaudeEventExtractor
from src.prediction.xgb_predictor import XGBPredictor
from src.risk.risk_calculator import RiskCalculator
from src.sentiment.finbert_service import FinBERTService
from src.sentiment.ner_service import NERService

logger = structlog.get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting FinStreamAI AI Services", port=settings.AI_SERVICES_PORT)

    # ── Instantiate service singletons ─────────────────────────────
    finbert = FinBERTService()
    ner = NERService()
    extractor = ClaudeEventExtractor()
    chroma = ChromaService(persist_dir=settings.CHROMA_PERSIST_DIR)
    predictor = XGBPredictor()
    risk = RiskCalculator()

    app.state.finbert = finbert
    app.state.ner = ner
    app.state.extractor = extractor
    app.state.chroma = chroma
    app.state.predictor = predictor
    app.state.risk = risk

    # ── Kafka consumer threads (daemon — stop when process exits) ───
    sentiment_consumer = SentimentConsumer(finbert)
    events_consumer = EventsConsumer(extractor)

    threads = [
        threading.Thread(target=sentiment_consumer.run, name="sentiment-consumer", daemon=True),
        threading.Thread(target=events_consumer.run, name="events-consumer", daemon=True),
    ]
    for t in threads:
        t.start()
        logger.info("Background thread started", name=t.name)

    # ── XGBoost initial train + hourly retrain scheduler ───────────
    def _initial_train():
        time.sleep(5)  # brief delay to let the service finish startup
        for sym in settings.watched_symbols_list:
            try:
                predictor.train(sym)
            except Exception as exc:
                logger.warning("Initial XGB train failed", symbol=sym, error=str(exc))
        predictor.start_retrain_scheduler()

    threading.Thread(target=_initial_train, name="xgb-init-train", daemon=True).start()

    logger.info("AI Services ready")
    yield

    # ── Shutdown ────────────────────────────────────────────────────
    sentiment_consumer.shutdown()
    events_consumer.shutdown()
    logger.info("AI Services stopped")


app = FastAPI(
    title="FinStreamAI — AI Services",
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
from src.api import embeddings, events, health, prediction, risk, sentiment  # noqa: E402

app.include_router(health.router, prefix=settings.API_V1_PREFIX)
app.include_router(sentiment.router, prefix=settings.API_V1_PREFIX)
app.include_router(events.router, prefix=settings.API_V1_PREFIX)
app.include_router(embeddings.router, prefix=settings.API_V1_PREFIX)
app.include_router(prediction.router, prefix=settings.API_V1_PREFIX)
app.include_router(risk.router, prefix=settings.API_V1_PREFIX)

# Root-level shortcuts
app.add_api_route("/health", health.health, include_in_schema=False)
app.add_api_route("/ready", health.ready, include_in_schema=False)
app.add_api_route("/live", health.live, include_in_schema=False)


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    logger.error("Unhandled exception", path=request.url.path, error=str(exc))
    return JSONResponse({"detail": "Internal server error"}, status_code=500)
