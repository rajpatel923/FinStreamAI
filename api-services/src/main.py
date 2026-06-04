import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware
from fastapi.responses import JSONResponse
from prometheus_client import make_asgi_app

from src.core.config import settings
from src.core.database import close_db, init_db
from src.core.logging import configure_logging, get_logger
from src.middleware.auth import JWTAuthMiddleware

configure_logging()
logger = get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting FinStreamAI API", version="0.1.0")
    await init_db()
    yield
    await close_db()
    logger.info("FinStreamAI API shut down")


app = FastAPI(
    title=settings.APP_NAME,
    version="0.1.0",
    docs_url="/docs" if settings.DEBUG else None,
    redoc_url="/redoc" if settings.DEBUG else None,
    openapi_url="/openapi.json" if settings.DEBUG else None,
    lifespan=lifespan,
)

# Middleware
app.add_middleware(TrustedHostMiddleware, allowed_hosts=["*"])
app.add_middleware(JWTAuthMiddleware, secret_key=settings.JWT_SECRET_KEY)
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ORIGINS,
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


# Prometheus metrics endpoint
metrics_app = make_asgi_app()
app.mount("/metrics", metrics_app)

# Routes
from src.api.v1 import health  # noqa: E402

app.include_router(health.router, prefix=settings.API_V1_PREFIX)

# Convenience redirects at root level
app.add_api_route("/health", health.health, include_in_schema=False)
app.add_api_route("/ready", health.readiness, include_in_schema=False)
app.add_api_route("/live", health.liveness, include_in_schema=False)


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    logger.error("Unhandled exception", path=request.url.path, error=str(exc))
    return JSONResponse({"detail": "Internal server error"}, status_code=500)
