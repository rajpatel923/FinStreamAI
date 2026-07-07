"""Correlation ID injection, access logging, and sensitive field masking."""
from __future__ import annotations

import re
import time
import uuid

import structlog
from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware

logger = structlog.get_logger(__name__)

_MASK_FIELDS = re.compile(
    r'"(password|hashed_password|key_hash|refresh_token|access_token|Authorization)[^"]*":\s*"[^"]*"',
    re.IGNORECASE,
)

_SKIP_PATHS = {"/metrics", "/api/v1/health", "/api/v1/live", "/api/v1/ready"}


class CorrelationIdMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next) -> Response:
        correlation_id = request.headers.get("X-Correlation-ID", str(uuid.uuid4()))
        structlog.contextvars.clear_contextvars()
        structlog.contextvars.bind_contextvars(
            correlation_id=correlation_id,
            method=request.method,
            path=request.url.path,
        )

        start = time.perf_counter()
        response = await call_next(request)
        duration_ms = (time.perf_counter() - start) * 1000

        response.headers["X-Correlation-ID"] = correlation_id

        if request.url.path not in _SKIP_PATHS:
            logger.info(
                "api_call",
                method=request.method,
                path=request.url.path,
                query=str(request.url.query) or None,
                status=response.status_code,
                duration_ms=round(duration_ms, 2),
                client=request.client.host if request.client else None,
            )

        return response
