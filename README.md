# FinStreamAI

A real-time financial data streaming and AI analysis platform. It ingests market data, news, and social signals from multiple sources, processes them through a stream pipeline, enriches them with AI (sentiment, NLP events, embeddings, predictions), stores everything in a multi-layer data lake, and exposes it all via a secure API gateway with an AI agent interface.

---

## Architecture Overview

```
                        ┌─────────────────────────────────────────────┐
                        │              Data Sources                   │
                        │  Polygon · Reddit · RSS Feeds · SEC EDGAR   │
                        └───────────────────┬─────────────────────────┘
                                            │
                              ┌─────────────▼──────────────┐
                              │      data-ingestion        │
                              │  Kafka producers (Avro)    │
                              │  18 topics · mock mode     │
                              └─────────────┬──────────────┘
                                            │ Kafka
                        ┌───────────────────▼──────────────────────┐
                        │           stream-processing               │
                        │  Clean · Aggregate · Anomaly Detection   │
                        │  Feature Engineering · Signal Generation  │
                        └──────┬────────────────────────┬──────────┘
                               │                        │
               ┌───────────────▼──────┐   ┌────────────▼──────────────┐
               │    ai-services       │   │       data-lake           │
               │  FinBERT sentiment   │   │  Bronze/Silver/Gold Delta │
               │  NER · Claude events │   │  Neo4j knowledge graph    │
               │  XGBoost predictions │   │  MinIO object storage     │
               │  ChromaDB embeddings │   │  Unified query layer      │
               └───────────┬──────────┘   └────────────┬──────────────┘
                           │                           │
                    ┌──────▼───────────────────────────▼───────┐
                    │              api-gateway                  │
                    │  Auth (JWT/OAuth/API keys) · Alerts       │
                    │  WebSocket · Export · Analytics           │
                    │  Watchlist · User preferences             │
                    └──────────────────┬────────────────────────┘
                                       │
                          ┌────────────▼────────────┐
                          │      agent-service       │
                          │  LangGraph supervisor    │
                          │  Portfolio advisor       │
                          │  Trade executor          │
                          │  Watchlist monitor       │
                          └─────────────────────────┘
```

---

## Services

| Service | Port | Description |
|---|---|---|
| `api-services` | 8000 | Core FastAPI app — health, DB pool, structured logging |
| `ai-services` | 8003 | FinBERT sentiment, NER, Claude event extraction, XGBoost predictions, ChromaDB embeddings |
| `data-lake` | 8004 | Delta Lake (Bronze/Silver/Gold), Neo4j knowledge graph, unified query |
| `api-gateway` | 8005 | Auth, alerts, WebSocket bridge, export, analytics, watchlist |
| `agent-service` | 8006 | LangGraph AI agent — portfolio advice, trade execution, watchlist monitoring |
| `data-ingestion` | — | Kafka producers: market ticks, bars, news, social, SEC filings |
| `stream-processing` | — | Kafka consumers: cleaning, aggregation, anomaly detection, signals |

---

## Tech Stack

| Layer | Technology |
|---|---|
| API framework | FastAPI + Python 3.11 |
| AI / ML | Claude (Anthropic), FinBERT, XGBoost, SHAP, ChromaDB, spaCy |
| Agent framework | LangGraph + LangChain |
| Message broker | Apache Kafka (Confluent) + Schema Registry (Avro) |
| Time-series DB | TimescaleDB (PostgreSQL 15) on port 5433 |
| Relational DB | PostgreSQL 15 on port 5434 |
| Cache | Redis 7 |
| Graph DB | Neo4j 5 |
| Object storage | MinIO (local S3-compatible) |
| Data lake | Delta Lake (delta-rs, no Spark) |
| Auth | JWT HS256 + bcrypt + Google/GitHub OAuth + API keys |
| Monitoring | Prometheus + Grafana + Jaeger |
| Infrastructure | Terraform + AWS EKS (scaffold) |

---

## Prerequisites

- **Docker** + **Docker Compose** v2
- **Python 3.11+**
- **uv** (Python package manager — replaces pip for virtual environments)
- **process-compose** (for local multi-service dev)

```bash
# Install uv (all platforms)
curl -LsSf https://astral.sh/uv/install.sh | sh

# macOS — Docker and process-compose
brew install docker
brew install f1bonacc1/process-compose/process-compose
```

---

## First-Time Setup

### 1. Clone and configure environment

```bash
git clone https://github.com/your-org/FinStreamAI.git
cd FinStreamAI
cp .env.example .env
```

Open `.env` and fill in at minimum:

```
JWT_SECRET_KEY=<random 32+ char string>
ANTHROPIC_API_KEY=<your key>          # required for ai-services + agent-service
POLYGON_API_KEY=<your key>            # or leave blank to use mock data
```

Everything else has working defaults for local dev.

### 2. Start infrastructure

```bash
make dev-infra
```

This starts: Kafka, Schema Registry, PostgreSQL, TimescaleDB, Redis, Neo4j, MinIO, Prometheus, Grafana, Jaeger.

### 3. Initialise databases and topics

```bash
make db-init        # PostgreSQL + TimescaleDB schemas
make lake-init      # MinIO buckets + Neo4j constraints/indexes
make topics         # Create all 18 Kafka topics
make migrate        # Run Alembic migrations (api-gateway)
```

### 4. Set up Python virtualenvs (one-time)

```bash
make venv-setup
```

This uses `uv` to create a `.venv/` inside each of the 7 service directories and install all dependencies. `uv` resolves conflicts faster and more reliably than pip. `ai-services` installs PyTorch and will take a few minutes on first run.

---

## Running Locally

### Option A — All services at once (recommended)

```bash
make dev-local
```

Opens the **process-compose TUI**: all 7 services start in dependency order with health checks. Use arrow keys to select a service and view its live logs.

```
┌─ process-compose ────────────────────────────────────────────┐
│ NAME               STATUS     RESTARTS   EXIT CODE           │
│ infra              Running    0          -                    │
│ api-services       Running    0          -                    │
│ data-ingestion     Running    0          -                    │
│ stream-processing  Running    0          -                    │
│ ai-services        Running    0          -                    │
│ data-lake          Running    0          -                    │
│ api-gateway        Running    0          -                    │
│ agent-service      Running    0          -                    │
└──────────────────────────────────────────────────────────────┘
```

### Option B — Individual services (for focused development)

```bash
make dev-infra      # start infra in Docker

# then in separate terminals, each with hot reload:
make api            # api-services  :8000
make ai             # ai-services   :8003
make lake           # data-lake     :8004
make gateway        # api-gateway   :8005
make agent          # agent-service :8006
make ingest         # data-ingestion (background pipeline)
make stream         # stream-processing (background pipeline)
```

---

## Service URLs

| Service | URL |
|---|---|
| api-services docs | http://localhost:8000/docs |
| ai-services docs | http://localhost:8003/docs |
| data-lake docs | http://localhost:8004/docs |
| api-gateway docs | http://localhost:8005/docs |
| agent-service docs | http://localhost:8006/docs |
| Grafana | http://localhost:3001 |
| Prometheus | http://localhost:9090 |
| Jaeger (tracing) | http://localhost:16686 |
| MinIO console | http://localhost:9001 |
| Neo4j browser | http://localhost:7474 |
| Schema Registry | http://localhost:8081 |
| Kafka | localhost:9092 |

---

## Testing

Run tests for each service individually or all at once:

```bash
make test           # api-services
make test-ingest    # data-ingestion
make test-stream    # stream-processing  (≥80% coverage required)
make test-ai        # ai-services        (≥80% coverage required)
make test-lake      # data-lake          (≥80% coverage required)
make test-gateway   # api-gateway        (≥85% coverage required)
make test-agent     # agent-service      (≥85% coverage required)
```

Tests use SQLite in-memory and mocked external dependencies (Kafka, Redis, Anthropic, Alpaca) — no running infrastructure needed.

---

## Health Checks

```bash
make health                           # check all service endpoints at once

# or individually:
curl http://localhost:8000/health     # api-services
curl http://localhost:8003/health     # ai-services
curl http://localhost:8004/health     # data-lake
curl http://localhost:8005/health     # api-gateway
curl http://localhost:8006/health     # agent-service
```

---

## All Make Commands

| Command | Description |
|---|---|
| `make setup` | Full setup: prereq check, start infra, create topics |
| `make venv-setup` | Create `.venv` + install deps for all 7 services via uv (one-time) |
| `make dev` | Start everything in Docker (no local Python processes) |
| `make dev-local` | Run all services locally via process-compose TUI |
| `make dev-infra` | Start infrastructure containers only |
| `make start` | Start full stack via start.sh |
| `make stop` | Stop all running services |
| `make restart` | stop + start |
| `make health` | Hit all health endpoints |
| `make migrate` | Run Alembic migrations (api-gateway) |
| `make db-init` | Re-run PostgreSQL + TimescaleDB init SQL |
| `make lake-init` | Init MinIO buckets + Neo4j schema |
| `make topics` | Create all 18 Kafka topics |
| `make timescaledb-optimize` | Apply continuous aggregates + compression |
| `make neo4j-init` | Run Neo4j constraints + indexes cypher |
| `make test` | Run api-services tests |
| `make test-{service}` | Run tests for a specific service |
| `make lint` | ruff + black check across all services |
| `make format` | Auto-format with black + isort |
| `make api` | Start api-services locally (hot reload) |
| `make ai` | Start ai-services locally (hot reload) |
| `make lake` | Start data-lake locally (hot reload) |
| `make gateway` | Start api-gateway locally (hot reload) |
| `make agent` | Start agent-service locally (hot reload) |
| `make ingest` | Start data-ingestion pipeline |
| `make stream` | Start stream-processing pipeline |
| `make logs` | Tail all Docker logs |
| `make logs-{service}` | Tail a specific service log file |
| `make kafka-ui` | Start Kafka UI on port 8080 |
| `make clean` | Stop everything + remove all volumes |

---

## Project Structure

```
FinStreamAI/
├── api-services/           Core FastAPI app (health, DB, structured logging)
├── data-ingestion/         Kafka producers (market, news, social, SEC)
│   └── src/
│       ├── producers/      BaseProducer + 5 concrete producers
│       ├── schemas/avro/   5 Avro schemas
│       └── utils/          MockDataGenerator, DataValidator, TokenBucket
├── stream-processing/      Kafka stream jobs
│   └── src/
│       ├── jobs/           Clean, Aggregate, Anomaly, Features, Signals, Join
│       ├── processors/     DataCleaner, TickAggregator, AnomalyDetector
│       ├── state/          RollingState, WindowState
│       └── sinks/          KafkaSink, TimescaleDBSink, RedisSink
├── ai-services/            ML inference service
│   └── src/
│       ├── sentiment/      FinBERT, NER
│       ├── events/         Claude event extractor
│       ├── embeddings/     ChromaDB + sentence-transformers
│       ├── prediction/     XGBoost + SHAP + FeatureLoader
│       └── risk/           VaR, CVaR, Sharpe, MaxDrawdown
├── data-lake/              Multi-layer data lake
│   └── src/
│       ├── lake/           Bronze, Silver, Gold Delta layers
│       ├── connectors/     Kafka → Bronze sink
│       ├── graph/          Neo4j knowledge graph
│       ├── cache/          Redis cache-aside + write-through
│       ├── quality/        QualityChecker + Quarantine
│       ├── catalog/        PostgreSQL partition metadata
│       └── query/          UnifiedQuery (parallel fan-out)
├── api-gateway/            Auth + public API surface
│   └── src/
│       ├── core/           JWT, bcrypt, API keys, SSRF validator
│       ├── services/       Auth, OAuth, Alerts, WebSocket, Export, Watchlist
│       ├── api/v1/         REST routers
│       └── migrations/     Alembic versions
├── agent-service/          LangGraph AI agent
│   └── src/
│       ├── agents/         Supervisor, PortfolioAdvisor, TradeExecutor, WatchlistMonitor
│       ├── memory/         ConversationStore, LangGraph checkpointer
│       ├── services/       SignalRouter, MonitoringLoop, DigestService
│       └── api/v1/         SSE chat, portfolio, trading endpoints
├── finstream_config/       Shared config package
├── scripts/                SQL init, setup.sh, start.sh, stop.sh, health-check.sh
├── monitoring/             Prometheus config
├── infrastructure/         Terraform modules (VPC, EKS, RDS, Kafka, Redis, S3)
├── process-compose.yml     Local multi-service orchestration
├── docker-compose.yml      Full containerised environment
└── Makefile                All dev commands
```

---

## Kafka Topics (18)

| Topic | Partitions | Description |
|---|---|---|
| `market.ticks.raw` | 50 | Raw tick data from producers |
| `market.ticks.clean` | 50 | Cleaned ticks from stream-processing |
| `market.bars.1min` | 20 | 1-minute OHLCV bars |
| `market.bars.5min` | 10 | 5-minute OHLCV bars |
| `market.bars.15min` | 5 | 15-minute OHLCV bars |
| `market.bars.1hour` | 3 | Hourly OHLCV bars |
| `technical.indicators` | 20 | RSI, MACD, Bollinger Bands, etc. |
| `news.articles.raw` | 10 | Raw news from RSS feeds |
| `news.articles.scored` | 10 | News with sentiment scores |
| `social.posts.raw` | 15 | Reddit posts |
| `social.sentiment` | 15 | Scored social posts |
| `events.extracted` | 5 | Claude-extracted financial events |
| `alerts.anomalies` | 5 | Anomaly detection alerts |
| `predictions.signals` | 10 | XGBoost trade signals |
| `watchlist.signals` | 5 | Signals for watchlisted symbols |
| `agent.recommendations` | 5 | Agent-generated recommendations |
| `trade.orders.submitted` | 5 | Submitted trade orders |
| `trade.orders.filled` | 5 | Filled trade confirmations |
