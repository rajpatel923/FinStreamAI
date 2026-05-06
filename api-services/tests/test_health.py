from unittest.mock import AsyncMock, patch

import pytest
from httpx import ASGITransport, AsyncClient

from src.main import app


@pytest.fixture
async def client():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        yield c


@pytest.fixture(autouse=True)
def mock_db_init():
    with patch("src.core.database.init_db", new_callable=AsyncMock) as mock:
        yield mock


@pytest.fixture(autouse=True)
def mock_db_close():
    with patch("src.core.database.close_db", new_callable=AsyncMock) as mock:
        yield mock


class TestLiveness:
    async def test_live_returns_200(self, client: AsyncClient):
        response = await client.get("/live")
        assert response.status_code == 200

    async def test_live_returns_ok(self, client: AsyncClient):
        response = await client.get("/live")
        assert response.json() == {"status": "ok"}


class TestReadiness:
    async def test_ready_structure(self, client: AsyncClient):
        with (
            patch("src.api.v1.health._check_postgres", return_value="ok"),
            patch("src.api.v1.health._check_timescaledb", return_value="ok"),
            patch("src.api.v1.health._check_redis", return_value="ok"),
        ):
            response = await client.get("/ready")
        assert "status" in response.json()

    async def test_ready_200_when_all_ok(self, client: AsyncClient):
        with (
            patch("src.api.v1.health._check_postgres", return_value="ok"),
            patch("src.api.v1.health._check_timescaledb", return_value="ok"),
            patch("src.api.v1.health._check_redis", return_value="ok"),
        ):
            response = await client.get("/ready")
        assert response.status_code == 200
        assert response.json()["status"] == "ready"

    async def test_ready_503_when_dependency_down(self, client: AsyncClient):
        with (
            patch("src.api.v1.health._check_postgres", return_value="error"),
            patch("src.api.v1.health._check_timescaledb", return_value="ok"),
            patch("src.api.v1.health._check_redis", return_value="ok"),
        ):
            response = await client.get("/ready")
        assert response.status_code == 503
        assert response.json()["status"] == "not_ready"


class TestHealth:
    async def test_health_returns_200_when_healthy(self, client: AsyncClient):
        with (
            patch("src.api.v1.health._check_postgres", return_value="ok"),
            patch("src.api.v1.health._check_timescaledb", return_value="ok"),
            patch("src.api.v1.health._check_redis", return_value="ok"),
        ):
            response = await client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
        assert "uptime_seconds" in data
        assert data["dependencies"]["postgres"] == "ok"
        assert data["dependencies"]["timescaledb"] == "ok"
        assert data["dependencies"]["redis"] == "ok"

    async def test_health_returns_503_when_degraded(self, client: AsyncClient):
        with (
            patch("src.api.v1.health._check_postgres", return_value="ok"),
            patch("src.api.v1.health._check_timescaledb", return_value="error"),
            patch("src.api.v1.health._check_redis", return_value="ok"),
        ):
            response = await client.get("/health")
        assert response.status_code == 503
        assert response.json()["status"] == "degraded"

    async def test_health_v1_prefix_works(self, client: AsyncClient):
        with (
            patch("src.api.v1.health._check_postgres", return_value="ok"),
            patch("src.api.v1.health._check_timescaledb", return_value="ok"),
            patch("src.api.v1.health._check_redis", return_value="ok"),
        ):
            response = await client.get("/api/v1/health")
        assert response.status_code == 200

    async def test_process_time_header_present(self, client: AsyncClient):
        response = await client.get("/live")
        assert "x-process-time" in response.headers
