"""Health check endpoints."""
from __future__ import annotations

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

router = APIRouter(tags=["health"])


async def health(request: Request) -> JSONResponse:
    return JSONResponse({"status": "healthy", "service": "data-lake"})


async def ready(request: Request) -> JSONResponse:
    checks: dict[str, str] = {}

    cache = getattr(request.app.state, "cache", None)
    if cache:
        try:
            cache.client.ping()
            checks["redis"] = "ok"
        except Exception:
            checks["redis"] = "error"

    neo4j = getattr(request.app.state, "neo4j_client", None)
    if neo4j:
        try:
            neo4j.run("RETURN 1")
            checks["neo4j"] = "ok"
        except Exception:
            checks["neo4j"] = "error"

    all_ok = all(v == "ok" for v in checks.values())
    status_code = 200 if all_ok else 503
    return JSONResponse({"status": "ready" if all_ok else "degraded", "checks": checks}, status_code=status_code)


async def live(request: Request) -> JSONResponse:
    return JSONResponse({"status": "alive"})


@router.get("/health")
async def health_route(request: Request):
    return await health(request)


@router.get("/ready")
async def ready_route(request: Request):
    return await ready(request)


@router.get("/live")
async def live_route(request: Request):
    return await live(request)
