.PHONY: setup dev dev-infra start stop restart health status logs logs-api logs-ingest logs-stream logs-ai logs-lake logs-gateway logs-agent test test-ingest test-stream test-ai test-lake test-gateway test-agent lint format api ingest stream ai lake gateway agent kafka-ui topics db-init lake-init timescaledb-optimize neo4j-init migrate clean help

DOCKER_COMPOSE = docker compose
PYTHON = python3

help:
	@echo "FinStreamAI — Developer Commands"
	@echo ""
	@echo "  make setup        Run setup.sh (checks prereqs, starts infra, creates topics)"
	@echo "  make start        Start full stack (infra + all app services)"
	@echo "  make stop         Stop all services (app processes + docker infra)"
	@echo "  make restart      stop + start"
	@echo "  make health       Check all service health endpoints"
	@echo "  make status       Alias for health"
	@echo "  make dev          Start all docker-compose services"
	@echo "  make dev-infra    Start infrastructure only (no app services)"
	@echo "  make logs         Tail all docker service logs"
	@echo "  make logs-api     Tail api-services log file"
	@echo "  make logs-ingest  Tail data-ingestion log file"
	@echo "  make logs-stream  Tail stream-processing log file"
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
	@echo "  make ai           Start ai-services locally with uvicorn --reload"
	@echo "  make test-ai      Run ai-services tests (≥80% coverage)"
	@echo "  make logs-ai      Tail ai-services log file"
	@echo "  make lake         Start data-lake service locally with uvicorn --reload"
	@echo "  make test-lake    Run data-lake tests (≥80% coverage)"
	@echo "  make lake-init    Init MinIO buckets + Neo4j schema"
	@echo "  make timescaledb-optimize  Apply continuous aggregates + compression SQL"
	@echo "  make neo4j-init   Run init-neo4j.cypher constraints + indexes"
	@echo "  make logs-lake    Tail data-lake log file"
	@echo "  make gateway      Start api-gateway locally with uvicorn --reload"
	@echo "  make test-gateway Run api-gateway tests (≥85% coverage)"
	@echo "  make logs-gateway Tail api-gateway log file"
	@echo "  make agent        Start agent-service locally with uvicorn --reload"
	@echo "  make test-agent   Run agent-service tests (≥85% coverage)"
	@echo "  make logs-agent   Tail agent-service log file"
	@echo "  make migrate      Run Alembic migrations (api-gateway)"
	@echo "  make clean        Stop services, remove volumes, prune"

setup:
	@bash scripts/setup.sh

start:
	@bash scripts/start.sh

stop:
	@bash scripts/stop.sh

restart: stop start

health:
	@bash scripts/health-check.sh

status: health

dev:
	$(DOCKER_COMPOSE) up -d
	@echo "All services started. API at http://localhost:8000"

dev-infra:
	$(DOCKER_COMPOSE) up -d kafka schema-registry postgres timescaledb redis neo4j minio prometheus grafana jaeger
	@echo "Infrastructure started."
	@echo "  Kafka:          localhost:9092"
	@echo "  PostgreSQL:     localhost:5432"
	@echo "  TimescaleDB:    localhost:5433"
	@echo "  Redis:          localhost:6379"
	@echo "  Prometheus:     http://localhost:9090"
	@echo "  Grafana:        http://localhost:3001 ($${GRAFANA_ADMIN_USER:-admin}/$${GRAFANA_ADMIN_PASSWORD:-from .env})"
	@echo "  Jaeger:         http://localhost:16686"

logs:
	$(DOCKER_COMPOSE) logs -f

logs-api:
	@tail -f logs/api.log

logs-ingest:
	@tail -f logs/ingest.log

logs-stream:
	@tail -f logs/stream.log

logs-ai:
	@tail -f logs/ai.log

logs-lake:
	@tail -f logs/lake.log

logs-gateway:
	@tail -f logs/gateway.log

logs-agent:
	@tail -f logs/agent.log

test:
	cd api-services && $(PYTHON) -m pytest tests/ -v --cov=src --cov-report=term-missing

test-ingest:
	cd data-ingestion && $(PYTHON) -m pytest tests/ -v

lint:
	cd api-services && $(PYTHON) -m ruff check src/ tests/ && $(PYTHON) -m black --check src/ tests/
	cd data-ingestion && $(PYTHON) -m ruff check src/ && $(PYTHON) -m black --check src/
	cd stream-processing && $(PYTHON) -m ruff check src/ && $(PYTHON) -m black --check src/
	cd ai-services && $(PYTHON) -m ruff check src/ tests/ && $(PYTHON) -m black --check src/ tests/
	cd api-gateway && $(PYTHON) -m ruff check src/ tests/ && $(PYTHON) -m black --check src/ tests/

format:
	cd api-services && $(PYTHON) -m black src/ tests/ && $(PYTHON) -m isort src/ tests/
	cd data-ingestion && $(PYTHON) -m black src/ && $(PYTHON) -m isort src/

api:
	cd api-services && $(PYTHON) -m uvicorn src.main:app --reload --host 0.0.0.0 --port 8000

ingest:
	cd data-ingestion && $(PYTHON) -m src.main

stream:
	cd stream-processing && $(PYTHON) -m src.main

ai:
	cd ai-services && $(PYTHON) -m uvicorn src.main:app --reload --host 0.0.0.0 --port 8003

test-stream:
	cd stream-processing && $(PYTHON) -m pytest tests/ -v --cov=src --cov-report=term-missing --cov-fail-under=80

test-ai:
	cd ai-services && $(PYTHON) -m pytest tests/ -v --cov=src --cov-report=term-missing --cov-fail-under=80 --cov-config=.coveragerc

lake:
	cd data-lake && $(PYTHON) -m uvicorn src.main:app --reload --host 0.0.0.0 --port 8004

test-lake:
	cd data-lake && $(PYTHON) -m pytest tests/ -v --cov=src --cov-report=term-missing --cov-fail-under=80 --cov-config=.coveragerc

gateway:
	cd api-gateway && $(PYTHON) -m uvicorn src.main:app --reload --host 0.0.0.0 --port 8005

test-gateway:
	cd api-gateway && $(PYTHON) -m pytest tests/ -v --cov=src --cov-report=term-missing --cov-fail-under=85 --cov-config=.coveragerc

agent:
	cd agent-service && $(PYTHON) -m uvicorn src.main:app --reload --host 0.0.0.0 --port 8006

test-agent:
	cd agent-service && $(PYTHON) -m pytest tests/ -v --cov=src --cov-report=term-missing --cov-fail-under=85 --cov-config=.coveragerc

migrate:
	cd api-gateway && $(PYTHON) -m alembic upgrade head

lake-init:
	@echo "Initializing MinIO buckets..."
	@bash scripts/init-minio.sh
	@echo "Initializing Neo4j schema..."
	@$(MAKE) neo4j-init

timescaledb-optimize:
	@echo "Applying TimescaleDB optimizations..."
	docker exec finstreami-timescaledb psql -U timescale -d timescaledb -f /docker-entrypoint-initdb.d/optimize-timescaledb.sql || \
	docker exec finstreami-timescaledb psql -U $${TIMESCALEDB_USER:-timescale} -d $${TIMESCALEDB_DB:-timescaledb} \
		-c "$$(cat scripts/optimize-timescaledb.sql)"
	@echo "TimescaleDB optimization complete."

neo4j-init:
	@echo "Running Neo4j schema init..."
	docker exec finstreami-neo4j cypher-shell \
		-u $${NEO4J_USER:-neo4j} \
		-p $${NEO4J_PASSWORD:-finstreami123} \
		-f /var/lib/neo4j/scripts/init-neo4j.cypher 2>/dev/null || \
	cat scripts/init-neo4j.cypher | docker exec -i finstreami-neo4j cypher-shell \
		-u $${NEO4J_USER:-neo4j} \
		-p $${NEO4J_PASSWORD:-finstreami123}
	@echo "Neo4j schema init complete."

kafka-ui:
	$(DOCKER_COMPOSE) run --rm -p 8080:8080 \
		-e KAFKA_CLUSTERS_0_NAME=local \
		-e KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS=kafka:29092 \
		provectuslabs/kafka-ui:latest

topics:
	@bash scripts/create-topics.sh

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
