from __future__ import annotations

import time

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

router = APIRouter(tags=["health"])

_START_TIME = time.time()


@router.get("/health")
def health(request: Request) -> dict:
    """Service health — always 200 if the process is alive."""
    return {
        "status": "ok",
        "service": "ai-services",
        "uptime_s": round(time.time() - _START_TIME, 1),
    }


@router.get("/ready")
def ready(request: Request) -> JSONResponse:
    """Readiness — checks that at least FinBERT pipeline is loadable."""
    # We only check that the service object exists, not that the model is loaded
    # (model loads lazily on first request to keep startup fast).
    if not hasattr(request.app.state, "finbert"):
        return JSONResponse({"status": "not_ready"}, status_code=503)
    return JSONResponse({"status": "ready"})


@router.get("/live")
def live() -> dict:
    return {"status": "alive"}
