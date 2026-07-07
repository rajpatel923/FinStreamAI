"""Agent Service — FastAPI application entry point."""
from __future__ import annotations

import asyncio
import time
from contextlib import asynccontextmanager

import structlog
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from src.core.config import settings
from src.core.database import close_db, init_db

logger = structlog.get_logger(__name__)


def _build_chat_model(cfg):
    provider = cfg.LLM_PROVIDER.lower()
    if provider == "openrouter":
        from langchain_openai import ChatOpenAI
        return ChatOpenAI(
            model=cfg.OPENROUTER_MODEL,
            api_key=cfg.OPENROUTER_API_KEY,
            base_url=cfg.OPENROUTER_BASE_URL,
        )
    if provider == "llama_cpp":
        from langchain_openai import ChatOpenAI
        return ChatOpenAI(
            model=cfg.LLAMA_CPP_MODEL,
            api_key=cfg.LLAMA_CPP_API_KEY,
            base_url=cfg.LLAMA_CPP_BASE_URL,
        )
    from langchain_anthropic import ChatAnthropic
    return ChatAnthropic(
        model=cfg.ANTHROPIC_MODEL,
        api_key=cfg.ANTHROPIC_API_KEY,
    )


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting FinStreamAI Agent Service", port=settings.AGENT_SERVICE_PORT)
    await init_db()

    # ── Checkpointer (PostgreSQL in prod) ────────────────────────
    from src.memory.checkpointer import get_checkpointer
    checkpointer = get_checkpointer()

    # ── Chat model ───────────────────────────────────────────────
    try:
        chat_model = _build_chat_model(settings)
        app.state.chat_model = chat_model
        logger.info("Chat model initialized", provider=settings.LLM_PROVIDER)
    except Exception as exc:
        logger.warning("Chat model init failed", error=str(exc))
        app.state.chat_model = None

    # ── Supervisor graph ─────────────────────────────────────────
    try:
        from src.agents.supervisor import build_supervisor
        app.state.supervisor = build_supervisor(app.state.chat_model, checkpointer)
    except Exception as exc:
        logger.warning("Supervisor build failed", error=str(exc))
        app.state.supervisor = None

    # ── Signal router ────────────────────────────────────────────
    from src.services.signal_router import SignalRouterConsumer
    router_consumer = SignalRouterConsumer(
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        redis_url=settings.redis_url,
    )
    router_consumer.start()
    app.state.signal_router = router_consumer

    # ── Monitoring scheduler ─────────────────────────────────────
    from src.services.monitoring_loop import start_scheduler
    scheduler = start_scheduler(app.state, settings)
    app.state.scheduler = scheduler

    logger.info("Agent Service ready")
    yield

    # ── Cleanup ──────────────────────────────────────────────────
    router_consumer.stop()
    from src.services.monitoring_loop import stop_scheduler
    await stop_scheduler(scheduler)
    await close_db()
    logger.info("Agent Service stopped")


app = FastAPI(
    title="FinStreamAI — Agent Service",
    version="0.1.0",
    docs_url="/docs" if settings.DEBUG else None,
    redoc_url="/redoc" if settings.DEBUG else None,
    openapi_url="/openapi.json" if settings.DEBUG else None,
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ORIGINS or ["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def add_process_time(request: Request, call_next):
    start = time.perf_counter()
    response = await call_next(request)
    response.headers["X-Process-Time"] = f"{(time.perf_counter() - start) * 1000:.2f}ms"
    return response


# ─── Routes ───────────────────────────────────────────────────────────────────
from src.api.v1 import agent, portfolio, trading  # noqa: E402

_PREFIX = settings.API_V1_PREFIX
app.include_router(agent.router, prefix=_PREFIX)
app.include_router(portfolio.router, prefix=_PREFIX)
app.include_router(trading.router, prefix=_PREFIX)


@app.get("/health", include_in_schema=False)
@app.get("/api/v1/health", include_in_schema=True)
async def health():
    return {"status": "ok", "service": "agent-service"}


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    logger.error("Unhandled exception", path=request.url.path, error=str(exc))
    return JSONResponse({"detail": "Internal server error"}, status_code=500)
