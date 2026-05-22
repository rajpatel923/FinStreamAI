.PHONY: setup dev dev-infra stop logs test test-ingest test-stream lint format api ingest stream kafka-ui topics db-init clean help

DOCKER_COMPOSE = docker compose
PYTHON = python3

help:
	@echo "FinStreamAI — Developer Commands"
	@echo ""
	@echo "  make setup        Run setup.sh (checks prereqs, starts infra, creates topics)"
	@echo "  make dev          Start all docker-compose services"
	@echo "  make dev-infra    Start infrastructure only (no app services)"
	@echo "  make stop         Stop all services"
	@echo "  make logs         Tail all service logs"
	@echo "  make test         Run all tests"
	@echo "  make lint         Run ruff + black check"
	@echo "  make format       Auto-format with black + isort"
	@echo "  make api          Start api-services locally with uvicorn --reload"
	@echo "  make kafka-ui     Start Kafka UI on port 8080"
	@echo "  make topics       Create all Kafka topics"
	@echo "  make db-init      Run database init scripts"
	@echo "  make ingest       Start data-ingestion pipeline (mock mode by default)"
	@echo "  make test-ingest  Run data-ingestion tests"
	@echo "  make stream       Start stream-processing pipeline"
	@echo "  make test-stream  Run stream-processing tests (≥80% coverage)"
	@echo "  make clean        Stop services, remove volumes, prune"

setup:
	@bash scripts/setup.sh

dev:
	$(DOCKER_COMPOSE) up -d
	@echo "All services started. API at http://localhost:8000"

dev-infra:
	$(DOCKER_COMPOSE) up -d zookeeper kafka schema-registry postgres timescaledb redis neo4j minio prometheus grafana jaeger
	@echo "Infrastructure started."
	@echo "  Kafka:          localhost:9092"
	@echo "  PostgreSQL:     localhost:5432"
	@echo "  TimescaleDB:    localhost:5433"
	@echo "  Redis:          localhost:6379"
	@echo "  Prometheus:     http://localhost:9090"
	@echo "  Grafana:        http://localhost:3001 ($${GRAFANA_ADMIN_USER:-admin}/$${GRAFANA_ADMIN_PASSWORD:-from .env})"
	@echo "  Jaeger:         http://localhost:16686"

stop:
	$(DOCKER_COMPOSE) down

logs:
	$(DOCKER_COMPOSE) logs -f

test:
	cd api-services && $(PYTHON) -m pytest tests/ -v --cov=src --cov-report=term-missing

test-ingest:
	cd data-ingestion && $(PYTHON) -m pytest tests/ -v

lint:
	cd api-services && $(PYTHON) -m ruff check src/ tests/ && $(PYTHON) -m black --check src/ tests/
	cd data-ingestion && $(PYTHON) -m ruff check src/ && $(PYTHON) -m black --check src/
	cd stream-processing && $(PYTHON) -m ruff check src/ && $(PYTHON) -m black --check src/

format:
	cd api-services && $(PYTHON) -m black src/ tests/ && $(PYTHON) -m isort src/ tests/
	cd data-ingestion && $(PYTHON) -m black src/ && $(PYTHON) -m isort src/

api:
	cd api-services && $(PYTHON) -m uvicorn src.main:app --reload --host 0.0.0.0 --port 8000

ingest:
	cd data-ingestion && $(PYTHON) -m src.main

stream:
	cd stream-processing && $(PYTHON) -m src.main

test-stream:
	cd stream-processing && $(PYTHON) -m pytest tests/ -v --cov=src --cov-report=term-missing --cov-fail-under=80

kafka-ui:
	$(DOCKER_COMPOSE) run --rm -p 8080:8080 \
		-e KAFKA_CLUSTERS_0_NAME=local \
		-e KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS=kafka:29092 \
		provectuslabs/kafka-ui:latest

topics:
	@bash scripts/setup.sh --topics-only

db-init:
	@echo "Initializing PostgreSQL..."
	docker exec finstreami-postgres psql -U finstreami -d finstreami -f /docker-entrypoint-initdb.d/init-db.sql
	@echo "Initializing TimescaleDB..."
	docker exec finstreami-timescaledb psql -U timescale -d timescaledb -f /docker-entrypoint-initdb.d/init-timescaledb.sql
	@echo "Database initialization complete."

clean:
	$(DOCKER_COMPOSE) down -v
	docker system prune -f
	@echo "Environment cleaned."
