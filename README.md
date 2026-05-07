# FinStreamAI

Real-time financial data streaming platform — Phase 1 local development foundation.

## Stack

| Layer | Technology |
|---|---|
| API | FastAPI + Python 3.11 |
| Message broker | Apache Kafka (Confluent) |
| Time-series DB | TimescaleDB (PostgreSQL 15) |
| Relational DB | PostgreSQL 15 |
| Cache | Redis 7 |
| Graph DB | Neo4j 5.12 |
| Object storage | MinIO (local S3) |
| Monitoring | Prometheus + Grafana + Jaeger |
| Infrastructure | Terraform + AWS EKS |

## Quick Start

```bash
# Prerequisites: Docker, Docker Compose, Python 3.11+
make setup    # Start all infrastructure + create Kafka topics
make api      # Start FastAPI at http://localhost:8000
```

## Commands

| Command | Description |
|---|---|
| `make setup` | Full local setup (prereq check, start infra, create topics) |
| `make dev` | Start all docker-compose services |
| `make dev-infra` | Start infrastructure only |
| `make stop` | Stop all services |
| `make logs` | Tail all logs |
| `make test` | Run pytest with coverage |
| `make lint` | Run ruff + black check |
| `make format` | Auto-format with black + isort |
| `make api` | Run FastAPI locally with hot reload |
| `make topics` | Create Kafka topics |
| `make db-init` | Re-run DB init scripts |
| `make clean` | Stop + remove all volumes |

## Service URLs (local)

| Service | URL |
|---|---|
| API | http://localhost:8000 |
| API Docs | http://localhost:8000/docs |
| Prometheus | http://localhost:9090 |
| Grafana | http://localhost:3001 (`GRAFANA_ADMIN_USER` / `GRAFANA_ADMIN_PASSWORD`) |
| Jaeger | http://localhost:16686 |
| MinIO | http://localhost:9001 |
| Neo4j | http://localhost:7474 |
| Schema Registry | http://localhost:8081 |

## Project Structure

```
FinStreamAI/
├── api-services/         FastAPI application
│   └── src/
│       ├── core/         Config, DB, logging
│       └── api/v1/       Health endpoints
├── data-ingestion/       Kafka producers
│   └── src/
│       ├── config/       Kafka + data source config
│       ├── producers/    Abstract BaseProducer
│       └── utils/        Prometheus metrics
├── scripts/              DB init SQL, setup.sh
├── monitoring/           Prometheus config
├── infrastructure/       Terraform modules (AWS)
└── docker-compose.yml    Full local environment
```

## Health Checks

```bash
curl http://localhost:8000/live    # Liveness probe
curl http://localhost:8000/ready   # Readiness probe
curl http://localhost:8000/health  # Full dependency status
```
