"""slowapi rate limiter setup with per-role key functions."""
from __future__ import annotations

import structlog
from fastapi import Request
from slowapi import Limiter
from slowapi.util import get_remote_address

logger = structlog.get_logger(__name__)


def _rate_limit_key(request: Request) -> str:
    """Key function: authenticated requests keyed by user_id, else by IP."""
    user = getattr(request.state, "user", None)
    if user is not None:
        return f"user:{user.id}"
    return f"ip:{get_remote_address(request)}"


def _login_key(request: Request) -> str:
    return f"login:{get_remote_address(request)}"


limiter = Limiter(
    key_func=_rate_limit_key,
    default_limits=[],
    storage_uri=None,  # set in main.py after settings are loaded
)
