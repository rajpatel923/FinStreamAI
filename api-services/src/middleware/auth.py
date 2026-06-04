"""Lightweight JWT validation middleware for api-services.

Validates Bearer tokens on all paths except health/metrics/docs.
Passes X-User-Id and X-User-Role headers to downstream handlers.
Returns 401 for missing/invalid tokens (when JWT_SECRET_KEY is configured).
"""
from __future__ import annotations

import structlog
from fastapi import Request
from fastapi.responses import JSONResponse
from jose import JWTError, jwt
from starlette.middleware.base import BaseHTTPMiddleware

logger = structlog.get_logger(__name__)

_SKIP_PATHS = frozenset(
    {"/health", "/ready", "/live", "/metrics", "/docs", "/redoc", "/openapi.json"}
)


class JWTAuthMiddleware(BaseHTTPMiddleware):
    def __init__(self, app, secret_key: str, algorithm: str = "HS256") -> None:
        super().__init__(app)
        self._secret = secret_key
        self._algorithm = algorithm
        self._enabled = bool(secret_key)

    async def dispatch(self, request: Request, call_next):
        if not self._enabled:
            return await call_next(request)

        path = request.url.path
        if path in _SKIP_PATHS or path.startswith("/metrics"):
            return await call_next(request)

        auth_header = request.headers.get("Authorization", "")
        if not auth_header.startswith("Bearer "):
            return JSONResponse(
                {"detail": "Not authenticated"},
                status_code=401,
                headers={"WWW-Authenticate": "Bearer"},
            )

        token = auth_header[7:]
        try:
            payload = jwt.decode(token, self._secret, algorithms=["HS256"])
        except JWTError as e:
            logger.warning("JWT validation failed", path=path, error=str(e))
            return JSONResponse(
                {"detail": "Invalid or expired token"},
                status_code=401,
                headers={"WWW-Authenticate": "Bearer"},
            )

        request.state.user_id = payload.get("sub")
        request.state.user_role = payload.get("role")
        response = await call_next(request)
        response.headers["X-User-Id"] = payload.get("sub", "")
        response.headers["X-User-Role"] = payload.get("role", "")
        return response
