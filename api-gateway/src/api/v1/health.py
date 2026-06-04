"""Gateway health endpoints."""
from fastapi import APIRouter
from fastapi.responses import JSONResponse

router = APIRouter(prefix="/health", tags=["health"])


@router.get("")
async def health():
    return {"status": "ok", "service": "api-gateway"}


@router.get("/ready")
async def ready():
    return {"status": "ready"}


@router.get("/live")
async def live():
    return {"status": "alive"}
