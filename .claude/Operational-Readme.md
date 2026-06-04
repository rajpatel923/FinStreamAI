# FinStreamAI Operational Readme

This file summarizes how to run the currently implemented services, how to test them, and what is still missing through Phase 5 based on the current repository state.

## Scope

- This summary is based on the actual repo contents, not only the high-level project plan.
- The implementation appears to cover a working subset of Phases 1 to 4.
- Phase 5 is only partially represented and is mostly not implemented end to end.

## Important Reality Check

- `make start` does not start every implemented service.
- The default startup path starts:
  - infrastructure via Docker
  - `api-services`
  - `data-ingestion`
  - `stream-processing`
- `ai-services` must be started separately.
- `make test` does not run all tests. It only runs `api-services` tests.

## Recommended Local Run Order

## 1. Prerequisites

You need:

- Docker Desktop with `docker compose`
- Python 3.11
- `make`

## 2. Create one shared Python environment

The startup scripts call `python3` directly, so the simplest non-code-change approach is one shared virtual environment at the repo root.

```bash
cd /Users/rajpatel/Documents/GitHub/FinStreamAI
python3.11 -m venv .venv
source .venv/bin/activate
pip install -r api-services/requirements.txt
pip install -r data-ingestion/requirements.txt
pip install -r stream-processing/requirements.txt
pip install -r ai-services/requirements.txt
```

## 3. Start infrastructure

```bash
make setup
```

What this does:

- starts Kafka
- starts Schema Registry
- starts PostgreSQL
- starts TimescaleDB
- starts Redis
- starts Neo4j
- starts MinIO
- starts Prometheus
- starts Grafana
- starts Jaeger
- creates Kafka topics

## 4. Start the app services

```bash
make start
```

This starts:

- `api-services` on port `8000`
- `data-ingestion` metrics on port `8001`
- `stream-processing` metrics on port `8002`

## 5. Start AI services separately

```bash
make ai
```

This starts:

- `ai-services` on port `8003`

## Local URLs

- API: `http://localhost:8000`
- API docs: `http://localhost:8000/docs`
- Ingest metrics: `http://localhost:8001/metrics`
- Stream metrics: `http://localhost:8002/metrics`
- AI services: `http://localhost:8003`
- AI docs: `http://localhost:8003/docs`
- Prometheus: `http://localhost:9090`
- Grafana: `http://localhost:3001`
- Jaeger: `http://localhost:16686`
- Schema Registry: `http://localhost:8081`
- Neo4j: `http://localhost:7474`
- MinIO console: `http://localhost:9001`

## Health Checks

## Standard repo health check

```bash
make health
```

This checks:

- Kafka
- Schema Registry
- PostgreSQL
- TimescaleDB
- Redis
- Neo4j
- MinIO
- Prometheus
- Grafana
- Jaeger
- API
- ingestion metrics
- stream metrics

It does not check `ai-services`.

## AI health checks

Run these separately:

```bash
curl http://localhost:8003/health
curl http://localhost:8003/ready
curl http://localhost:8003/live
```

## Test Commands

Run all test suites explicitly:

```bash
make test
make test-ingest
make test-stream
make test-ai
```

What each command does:

- `make test` -> `api-services` tests only
- `make test-ingest` -> `data-ingestion` tests
- `make test-stream` -> `stream-processing` tests
- `make test-ai` -> `ai-services` tests

## Functional Verification Checklist

After startup, verify the following:

## 1. Kafka topics exist

```bash
docker exec finstreami-kafka kafka-topics --bootstrap-server localhost:9092 --list
```

Expected important topics:

- `market.ticks.raw`
- `market.ticks.clean`
- `market.bars.1min`
- `market.bars.5min`
- `market.bars.15min`
- `market.bars.1hour`
- `technical.indicators`
- `news.articles.raw`
- `news.articles.scored`
- `social.posts.raw`
- `social.sentiment`
- `events.extracted`
- `alerts.anomalies`
- `predictions.signals`

## 2. API is alive

```bash
curl http://localhost:8000/live
curl http://localhost:8000/ready
curl http://localhost:8000/health
```

## 3. Metrics endpoints respond

```bash
curl http://localhost:8001/metrics
curl http://localhost:8002/metrics
curl http://localhost:8003/metrics
```

## 4. Redis feature keys appear

```bash
docker exec finstreami-redis redis-cli -a redis123 keys 'finstreami:*'
```

If your `.env` uses a different password, use that instead of `redis123`.

## 5. TimescaleDB tables receive data

```bash
docker exec finstreami-timescaledb psql -U timescale -d timescaledb -c "select count(*) from market_ticks;"
docker exec finstreami-timescaledb psql -U timescale -d timescaledb -c "select count(*) from market_bars;"
docker exec finstreami-timescaledb psql -U timescale -d timescaledb -c "select count(*) from technical_indicators;"
docker exec finstreami-timescaledb psql -U timescale -d timescaledb -c "select count(*) from trading_signals;"
```

## 6. AI endpoints respond

Examples:

```bash
curl http://localhost:8003/health
curl -X POST http://localhost:8003/api/v1/sentiment/analyze -H 'Content-Type: application/json' -d '{"text":"Apple reported strong earnings and raised guidance."}'
curl -X POST http://localhost:8003/api/v1/events/extract -H 'Content-Type: application/json' -d '{"text":"Microsoft announced a new product launch for Azure AI."}'
curl http://localhost:8003/api/v1/predict/signals/AAPL
curl http://localhost:8003/api/v1/risk/metrics/AAPL
```

## Efficiency and Performance Validation

The repo does not currently include a full load-testing or benchmark suite. To judge whether services are working efficiently, use the following practical checks.

## 1. Resource usage

```bash
docker stats
```

Watch for:

- Kafka memory growth
- TimescaleDB CPU spikes
- Redis memory usage
- any constantly restarting container

## 2. Prometheus and metrics

Open:

- `http://localhost:9090`
- `http://localhost:3001`

Check whether:

- metrics are being scraped
- ingestion counters are increasing
- stream-processing counters are increasing
- request latency histograms are visible

## 3. Simple API latency smoke test

If `hey` is installed:

```bash
hey -n 500 -c 20 http://localhost:8000/health
hey -n 200 -c 10 http://localhost:8003/health
```

If `wrk` is installed:

```bash
wrk -t4 -c20 -d30s http://localhost:8000/health
wrk -t2 -c10 -d15s http://localhost:8003/health
```

## 4. Database query timing

Inside `psql`, enable timing:

```bash
docker exec -it finstreami-timescaledb psql -U timescale -d timescaledb
```

Then run:

```sql
\timing
select * from market_bars order by time desc limit 100;
select * from technical_indicators order by time desc limit 100;
```

## What Is Implemented Today

## Data ingestion

Currently implemented producers include:

- market data producer
- market bar producer
- news producer
- Reddit producer
- SEC filing producer

These can run in mock mode if real keys are missing.

## Stream processing

Currently implemented jobs include:

- data cleaning
- aggregation
- anomaly detection
- feature engineering
- signal generation
- real-time join

This is implemented as Python Kafka consumers, not Flink.

## AI services

Currently implemented AI-related features include:

- FinBERT sentiment API
- NER API
- Claude-based event extraction with spaCy fallback
- Chroma-based embeddings search
- XGBoost prediction service
- risk metrics service

## What Is Missing Through Phase 5

Below is the gap list relative to the phased plan in `.claude/main.md`.

## Phase 1 gaps

- No single bootstrap for all Python dependencies across services.
- `make test` is not truly "run all tests".
- `ai-services` is not part of the default `make start` flow.
- Prometheus targets for Postgres and Redis exporters are configured, but those exporters are not present in Docker Compose.

## Phase 2 gaps

- No Twitter/X ingestion
- No StockTwits ingestion
- No IEX Cloud ingestion
- No custom RSS ingestion pipeline
- No alternative data connectors
- No real websocket-first market streaming pipeline across all sources

## Phase 3 gaps

- No Apache Flink deployment
- No Flink checkpointing/savepoints
- No RocksDB state backend
- No Kubernetes-based Flink runtime
- No exactly-once Flink processing implementation

What exists instead:

- Python consumer threads reading Kafka and writing outputs

## Phase 4 gaps

- No Ray Serve cluster
- No GPU deployment setup
- No Weaviate deployment
- No Feast feature store
- No transformer-based multimodal prediction service as described in plan
- No robust model registry
- No production drift monitoring framework

What exists instead:

- local FastAPI AI service
- Chroma vector store
- XGBoost predictor
- risk calculator
- Anthropic-backed event extractor with spaCy fallback

## Phase 5 gaps

### Week 9 missing

- No Bronze/Silver/Gold data lake implementation
- No Kafka Connect S3 sink
- No Delta Lake
- No Spark transformation jobs
- No Parquet-based historical lake pipeline
- No data catalog
- No quarantine pipeline for bad records
- No scheduled Silver/Gold processing

### Week 10 partially present

Present:

- TimescaleDB hypertables
- indexes
- two continuous aggregates
- limited retention policies
- Redis feature writes
- Neo4j container exists

Missing:

- TimescaleDB compression strategy
- chunk interval tuning
- refresh policies
- broader retention rules for all key tables
- knowledge graph schema and loaders
- graph queries and graph algorithms
- Redis HA cluster design
- cache warming strategy
- unified query interface across storage systems
- performance benchmarking for 100+ concurrent query patterns

## Known Practical Issues

- `make start` assumes `python3` can import all service dependencies from one environment.
- `make health` may give a false sense of completeness because it does not include `ai-services`.
- `make dev` is not the same as a full application startup.
- Monitoring is only partially wired. Prometheus scraping is incomplete for some dependencies.
- Some docs describe more maturity than the actual implementation currently has.

## Safest Way To Evaluate The Current Repo

Use this process:

1. Start in mock mode.
2. Run `make setup`.
3. Run `make start`.
4. Run `make ai`.
5. Run all four test commands.
6. Check health endpoints.
7. Check Redis keys and TimescaleDB row counts.
8. Watch Prometheus, Grafana, and `docker stats`.
9. Only after that, try real API keys.

## Bottom Line

The repo looks like a working local prototype with:

- usable infrastructure
- ingestion
- stream-processing
- API service
- separate AI service

It does not yet look like a complete Phase 5 implementation. The largest missing areas are:

- true data lake architecture
- graph database usage
- unified query layer
- production-grade observability and performance validation
- full automation for running all services together
