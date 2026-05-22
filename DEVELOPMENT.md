# FinStreamAI — Developer Guide

## Prerequisites

| Tool | Version | Notes |
|------|---------|-------|
| Docker | 24+ | Docker Compose v2 required |
| Python | 3.11+ | Use a virtualenv per service |
| make | any | Standard on macOS/Linux |

---

## First-Time Setup

```bash
git clone https://github.com/rajpatel923/FinStreamAI.git
cd FinStreamAI
cp .env.example .env       # edit to add real API keys (optional — mock mode works without them)
make setup                 # starts infra, creates 14 Kafka topics
```

Then start all app services:

```bash
make start
```

---

## Daily Workflow

```bash
make start       # start everything (infra + api + ingestion + stream)
make health      # verify all services are up
make stop        # stop everything cleanly
make restart     # stop + start
```

---

## Running Tests

```bash
make test          # api-services (pytest + coverage)
make test-ingest   # data-ingestion
make test-stream   # stream-processing (requires ≥80% coverage)
```

---

## Service URLs

| Service | URL / Address |
|---------|--------------|
| FastAPI (app) | http://localhost:8000 |
| API Docs (Swagger) | http://localhost:8000/docs |
| Ingest Metrics | http://localhost:8001/metrics |
| Stream Metrics | http://localhost:8002/metrics |
| Kafka | localhost:9092 |
| Schema Registry | http://localhost:8081 |
| PostgreSQL | localhost:5434 |
| TimescaleDB | localhost:5433 |
| Redis | localhost:6379 |
| Neo4j | http://localhost:7474 |
| MinIO (console) | http://localhost:9001 |
| Prometheus | http://localhost:9090 |
| Grafana | http://localhost:3001 |
| Jaeger | http://localhost:16686 |

Default credentials are in `.env.example`.

---

## Architecture Overview

```
                         ┌─────────────────────────────────────────────────────┐
                         │                 FinStreamAI                          │
                         │                                                     │
  External APIs          │  data-ingestion        stream-processing            │
  ─────────────          │  ───────────────        ────────────────────        │
  Polygon.io     ──────► │  MarketDataProducer     DataCleaningJob             │
  Alpha Vantage  ──────► │  NewsProducer       ──► AggregationJob    ──► Redis │
  Reddit         ──────► │  SocialProducer         AnomalyDetection            │
  NewsAPI        ──────► │  SECFilingProducer       FeatureEngineering         │
                         │        │               SignalGeneration             │
                         │        ▼                      │                    │
                         │    Kafka (KRaft)               ▼                   │
                         │    14 topics          TimescaleDB (time-series)     │
                         │                                                     │
                         │  api-services                                       │
                         │  ────────────                                       │
                         │  FastAPI :8000 ◄── clients                         │
                         │  PostgreSQL (relational)                            │
                         └─────────────────────────────────────────────────────┘
```

---

## Logs

App service logs are written to `logs/` when started via `make start`:

```bash
make logs-api       # tail logs/api.log
make logs-ingest    # tail logs/ingest.log
make logs-stream    # tail logs/stream.log
make logs           # tail all Docker service logs
```

---

## Mock Mode

If `POLYGON_API_KEY` (and other market API keys) are not set or still contain placeholder values from `.env.example`, `start.sh` automatically sets `USE_MOCK_DATA=True`. Mock mode generates realistic synthetic market data — no external API accounts required.

To force real data: set valid API keys in `.env` before running `make start`.

---

## Troubleshooting

**Kafka not ready / topics not created**
```bash
docker logs finstreami-kafka
# If container is missing, run: make dev-infra
```

**TimescaleDB port conflict** (5433 already in use)
```bash
lsof -i :5433
# Kill the conflicting process, then: make start
```

**PostgreSQL port conflict** (5434 already in use)
```bash
lsof -i :5434
```

**Redis auth failure** (`NOAUTH Authentication required`)
```bash
# Ensure REDIS_PASSWORD in .env matches the password in docker-compose.yml
grep REDIS_PASSWORD .env
```

**App service failed to start (stale PID)**
```bash
rm -f logs/pids/*.pid
make start
```

**Schema Registry unreachable**
Schema Registry depends on Kafka. If Kafka is not yet healthy, Schema Registry will crash-loop. Wait for `make health` to show Kafka as OK, then restart:
```bash
docker compose restart schema-registry
```

**Port 8000 already in use**
```bash
lsof -i :8000
# Kill the process, then: make start
```
