#!/usr/bin/env bash
# Start the full FinStreamAI stack: infra (Docker) + app services (local Python).
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SCRIPT_DIR="$REPO_ROOT/scripts"
LOG_DIR="$REPO_ROOT/logs"
PID_DIR="$LOG_DIR/pids"

cd "$REPO_ROOT"

# ── Colors ──────────────────────────────────────────────────────────────────
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

info()  { echo -e "${GREEN}[start]${NC} $*"; }
warn()  { echo -e "${YELLOW}[start]${NC} $*"; }
error() { echo -e "${RED}[start]${NC} $*" >&2; }

# ── Prereqs ──────────────────────────────────────────────────────────────────
for cmd in docker python3; do
  if ! command -v "$cmd" &>/dev/null; then
    error "$cmd is required but not installed."
    exit 1
  fi
done

DOCKER_COMPOSE_CMD="docker compose"
if ! docker compose version &>/dev/null 2>&1; then
  DOCKER_COMPOSE_CMD="docker-compose"
fi

# ── Dirs ──────────────────────────────────────────────────────────────────────
mkdir -p "$LOG_DIR" "$PID_DIR"

# ── .env ──────────────────────────────────────────────────────────────────────
if [[ ! -f .env ]]; then
  warn ".env not found — copying from .env.example (edit to add real API keys)"
  cp .env.example .env
fi

# Read a single value from .env safely (no sourcing — avoids unquoted spaces).
_env_get() {
  local key="$1" default="${2:-}"
  local val
  val=$(grep -E "^${key}=" .env 2>/dev/null | head -1 | cut -d= -f2- | sed "s/^['\"]//;s/['\"]$//")
  echo "${val:-$default}"
}

# ── Mock data auto-detection ──────────────────────────────────────────────────
USE_MOCK_DATA="${USE_MOCK_DATA:-$(_env_get USE_MOCK_DATA False)}"
_polygon_key="$(_env_get POLYGON_API_KEY "")"
if [[ -z "$_polygon_key" || "$_polygon_key" == your_* ]]; then
  warn "No real API keys detected — enabling USE_MOCK_DATA=True"
  USE_MOCK_DATA="True"
fi
export USE_MOCK_DATA

echo ""
echo "============================================"
echo " FinStreamAI — Starting Full Stack"
echo "============================================"
echo ""

# ── Step 1: Infrastructure ────────────────────────────────────────────────────
info "Step 1/7 — Starting infrastructure services..."
$DOCKER_COMPOSE_CMD up -d kafka schema-registry postgres timescaledb redis neo4j minio prometheus grafana jaeger

# ── Step 2: Wait for Kafka ────────────────────────────────────────────────────
info "Step 2/7 — Waiting for Kafka..."
KAFKA_ATTEMPTS=0
until docker exec finstreami-kafka kafka-broker-api-versions --bootstrap-server localhost:9092 &>/dev/null 2>&1; do
  KAFKA_ATTEMPTS=$((KAFKA_ATTEMPTS + 1))
  if [[ $KAFKA_ATTEMPTS -gt 30 ]]; then
    error "Kafka did not become ready after 150s."
    error "Check logs: docker logs finstreami-kafka"
    exit 1
  fi
  echo "  Kafka not ready (attempt $KAFKA_ATTEMPTS/30)..."
  sleep 5
done
info "Kafka ready."

# ── Step 3: Wait for databases + Redis ───────────────────────────────────────
info "Step 3/7 — Waiting for TimescaleDB, PostgreSQL, Redis..."
until docker exec finstreami-timescaledb pg_isready -U timescale -d timescaledb &>/dev/null 2>&1; do
  sleep 3
done
until docker exec finstreami-postgres pg_isready -U finstreami -d finstreami &>/dev/null 2>&1; do
  sleep 3
done
REDIS_PASS="${REDIS_PASSWORD:-$(_env_get REDIS_PASSWORD redis123)}"
until docker exec finstreami-redis redis-cli -a "$REDIS_PASS" ping 2>/dev/null | grep -q PONG; do
  sleep 3
done
info "Databases and Redis ready."

# ── Step 4: Create Kafka topics ───────────────────────────────────────────────
info "Step 4/7 — Creating Kafka topics (idempotent)..."
bash "$SCRIPT_DIR/create-topics.sh"

# ── Step 5: api-services ──────────────────────────────────────────────────────
info "Step 5/7 — Starting api-services (port 8000)..."
API_PID_FILE="$PID_DIR/api.pid"
if [[ -f "$API_PID_FILE" ]] && kill -0 "$(cat "$API_PID_FILE")" 2>/dev/null; then
  warn "api-services already running (PID $(cat "$API_PID_FILE")) — skipping"
else
  cd "$REPO_ROOT/api-services"
  python3 -m uvicorn src.main:app --host 0.0.0.0 --port 8000 \
    >"$LOG_DIR/api.log" 2>&1 &
  echo $! >"$API_PID_FILE"
  info "api-services started (PID $!, logs/api.log)"
  cd "$REPO_ROOT"
fi

# ── Step 6: data-ingestion ────────────────────────────────────────────────────
info "Step 6/7 — Starting data-ingestion (metrics port 8001)..."
INGEST_PID_FILE="$PID_DIR/ingest.pid"
if [[ -f "$INGEST_PID_FILE" ]] && kill -0 "$(cat "$INGEST_PID_FILE")" 2>/dev/null; then
  warn "data-ingestion already running (PID $(cat "$INGEST_PID_FILE")) — skipping"
else
  cd "$REPO_ROOT/data-ingestion"
  python3 -m src.main >"$LOG_DIR/ingest.log" 2>&1 &
  echo $! >"$INGEST_PID_FILE"
  info "data-ingestion started (PID $!, logs/ingest.log)"
  cd "$REPO_ROOT"
fi

# ── Step 7: stream-processing ─────────────────────────────────────────────────
info "Step 7/7 — Starting stream-processing (metrics port 8002)..."
STREAM_PID_FILE="$PID_DIR/stream.pid"
if [[ -f "$STREAM_PID_FILE" ]] && kill -0 "$(cat "$STREAM_PID_FILE")" 2>/dev/null; then
  warn "stream-processing already running (PID $(cat "$STREAM_PID_FILE")) — skipping"
else
  cd "$REPO_ROOT/stream-processing"
  python3 -m src.main >"$LOG_DIR/stream.log" 2>&1 &
  echo $! >"$STREAM_PID_FILE"
  info "stream-processing started (PID $!, logs/stream.log)"
  cd "$REPO_ROOT"
fi

# ── Summary ───────────────────────────────────────────────────────────────────
echo ""
echo "============================================"
echo " FinStreamAI — Running"
echo "============================================"
echo ""
echo "Application:"
echo "  API (FastAPI):      http://localhost:8000"
echo "  API Docs:           http://localhost:8000/docs"
echo "  Ingest Metrics:     http://localhost:8001/metrics"
echo "  Stream Metrics:     http://localhost:8002/metrics"
echo ""
echo "Infrastructure:"
echo "  Kafka:              localhost:9092"
echo "  Schema Registry:    http://localhost:8081"
echo "  PostgreSQL:         localhost:5434"
echo "  TimescaleDB:        localhost:5433"
echo "  Redis:              localhost:6379"
echo "  Grafana:            http://localhost:3001"
echo "  Prometheus:         http://localhost:9090"
echo "  Jaeger:             http://localhost:16686"
echo "  MinIO:              http://localhost:9001"
echo ""
echo "Commands:"
echo "  make health         # check all services"
echo "  make logs-api       # tail api log"
echo "  make logs-ingest    # tail ingestion log"
echo "  make logs-stream    # tail stream log"
echo "  make stop           # stop everything"
echo ""
