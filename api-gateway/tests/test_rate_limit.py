"""Tests for rate limiting: SSRF validation, security helpers."""
from __future__ import annotations

import pytest

from src.core.security import validate_webhook_url


class TestSSRFValidation:
    def test_public_url_allowed(self):
        assert validate_webhook_url("https://hooks.slack.com/services/TOKEN") is True

    def test_loopback_blocked(self):
        assert validate_webhook_url("http://127.0.0.1/internal") is False

    def test_rfc1918_10_blocked(self):
        assert validate_webhook_url("http://10.0.0.1/api") is False

    def test_rfc1918_172_blocked(self):
        assert validate_webhook_url("http://172.20.0.1/api") is False

    def test_rfc1918_192168_blocked(self):
        assert validate_webhook_url("http://192.168.1.100/api") is False

    def test_link_local_blocked(self):
        assert validate_webhook_url("http://169.254.169.254/metadata") is False

    def test_localhost_hostname_blocked(self):
        assert validate_webhook_url("http://localhost/internal") is False

    def test_local_hostname_blocked(self):
        assert validate_webhook_url("http://internal.local/api") is False

    def test_non_http_scheme_blocked(self):
        assert validate_webhook_url("ftp://files.example.com/hook") is False

    def test_empty_url_blocked(self):
        assert validate_webhook_url("") is False

    def test_public_http_allowed(self):
        assert validate_webhook_url("http://webhook.example.com/hook") is True


class TestSecurityHelpers:
    def test_hash_password_different_each_time(self):
        from src.core.security import hash_password, verify_password
        h1 = hash_password("mysecretpassword")
        h2 = hash_password("mysecretpassword")
        assert h1 != h2  # bcrypt salts
        assert verify_password("mysecretpassword", h1)
        assert verify_password("mysecretpassword", h2)
        assert not verify_password("wrongpassword", h1)

    def test_generate_api_key_format(self):
        from src.core.security import generate_api_key, verify_api_key
        full_key, prefix, key_hash = generate_api_key()
        assert full_key.startswith("fsk_")
        assert len(prefix) == 10
        assert prefix == full_key[:10]
        assert verify_api_key(full_key, key_hash)

    def test_jwt_encode_decode(self):
        from src.core.config import settings
        settings.JWT_SECRET_KEY = "test_secret_key_for_tests"
        from src.core.security import create_access_token, decode_access_token
        token = create_access_token("user-123", "free_user")
        payload = decode_access_token(token)
        assert payload["sub"] == "user-123"
        assert payload["role"] == "free_user"

    def test_jwt_decode_invalid(self):
        from src.core.config import settings
        settings.JWT_SECRET_KEY = "test_secret_key_for_tests"
        from src.core.security import decode_access_token
        from jose import JWTError
        with pytest.raises(JWTError):
            decode_access_token("invalid.jwt.token")

    def test_refresh_token_hash(self):
        from src.core.security import generate_refresh_token, hash_refresh_token
        token = generate_refresh_token()
        h1 = hash_refresh_token(token)
        h2 = hash_refresh_token(token)
        assert h1 == h2  # deterministic SHA-256
        assert len(token) > 50  # sufficiently long

    def test_oauth_state_unique(self):
        from src.core.security import generate_oauth_state
        states = {generate_oauth_state() for _ in range(10)}
        assert len(states) == 10  # all unique
