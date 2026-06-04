"""Security utilities: JWT, bcrypt, API key generation, SSRF validation."""
from __future__ import annotations

import hashlib
import ipaddress
import os
import re
import secrets
import uuid
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse

from jose import JWTError, jwt
from passlib.context import CryptContext

from .config import settings

_pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# RFC1918 + loopback + link-local ranges blocked for SSRF
_BLOCKED_NETWORKS = [
    ipaddress.ip_network("10.0.0.0/8"),
    ipaddress.ip_network("172.16.0.0/12"),
    ipaddress.ip_network("192.168.0.0/16"),
    ipaddress.ip_network("127.0.0.0/8"),
    ipaddress.ip_network("169.254.0.0/16"),
    ipaddress.ip_network("::1/128"),
    ipaddress.ip_network("fc00::/7"),
    ipaddress.ip_network("fe80::/10"),
]


# ─── Password ────────────────────────────────────────────────────────────────

def hash_password(plain: str) -> str:
    return _pwd_context.hash(plain)


def verify_password(plain: str, hashed: str) -> bool:
    return _pwd_context.verify(plain, hashed)


# ─── JWT ─────────────────────────────────────────────────────────────────────

def create_access_token(user_id: str, role: str) -> str:
    now = datetime.now(timezone.utc)
    payload = {
        "sub": user_id,
        "role": role,
        "jti": str(uuid.uuid4()),
        "iat": now,
        "exp": now + timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES),
    }
    return jwt.encode(payload, settings.JWT_SECRET_KEY, algorithm="HS256")


def decode_access_token(token: str) -> dict:
    """Decode and validate an access token. Raises JWTError on failure."""
    return jwt.decode(
        token,
        settings.JWT_SECRET_KEY,
        algorithms=["HS256"],  # hardcoded — reject alg=none
    )


# ─── Refresh token ───────────────────────────────────────────────────────────

def generate_refresh_token() -> str:
    """Return a cryptographically random base64url-encoded 512-bit token."""
    return secrets.token_urlsafe(64)


def hash_refresh_token(token: str) -> str:
    return hashlib.sha256(token.encode()).hexdigest()


# ─── API keys ────────────────────────────────────────────────────────────────

def generate_api_key() -> tuple[str, str, str]:
    """Return (full_key, key_prefix, key_hash)."""
    raw = secrets.token_hex(32)
    full_key = f"fsk_{raw}"
    key_prefix = full_key[:10]
    key_hash = _pwd_context.hash(full_key)
    return full_key, key_prefix, key_hash


def verify_api_key(plain: str, hashed: str) -> bool:
    return _pwd_context.verify(plain, hashed)


# ─── SSRF prevention ─────────────────────────────────────────────────────────

def validate_webhook_url(url: str) -> bool:
    """Return True if the URL is safe (public internet), False if SSRF risk."""
    try:
        parsed = urlparse(url)
        if parsed.scheme not in ("http", "https"):
            return False
        hostname = parsed.hostname
        if not hostname:
            return False
        # Resolve to IP — raises ValueError for invalid addresses
        try:
            addr = ipaddress.ip_address(hostname)
        except ValueError:
            # It's a hostname — do a basic regex check; production would DNS-resolve
            # Block obvious private hostnames
            if re.match(r"^(localhost|.*\.local|.*\.internal)$", hostname, re.IGNORECASE):
                return False
            return True
        for network in _BLOCKED_NETWORKS:
            if addr in network:
                return False
        return True
    except Exception:
        return False


# ─── OAuth state ─────────────────────────────────────────────────────────────

def generate_oauth_state() -> str:
    return secrets.token_urlsafe(32)
