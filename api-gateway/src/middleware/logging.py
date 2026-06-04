"""Correlation ID injection and sensitive field masking."""
from __future__ import annotations

import re
import uuid

import structlog
from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware

logger = structlog.get_logger(__name__)

_MASK_FIELDS = re.compile(
    r'"(password|hashed_password|key_hash|refresh_token|access_token|Authorization)[^"]*":\s*"[^"]*"',
    re.IGNORECASE,
)


class CorrelationIdMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next) -> Response:
        correlation_id = request.headers.get("X-Correlation-ID", str(uuid.uuid4()))
        structlog.contextvars.clear_contextvars()
        structlog.contextvars.bind_contextvars(
            correlation_id=correlation_id,
            method=request.method,
            path=request.url.path,
        )
        response = await call_next(request)
        response.headers["X-Correlation-ID"] = correlation_id
        return response
