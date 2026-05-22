#!/usr/bin/env bash
# Check health of all FinStreamAI services. Exits 0 only if all pass.
set -uo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

GREEN='\033[0;32m'
RED='\033[0;31m'
BOLD='\033[1m'
NC='\033[0m'

PASS=true

# Read REDIS_PASSWORD from .env safely (no sourcing — avoids unquoted spaces).
REDIS_PASS="redis123"
if [[ -f "$REPO_ROOT/.env" ]]; then
  _val=$(grep -E "^REDIS_PASSWORD=" "$REPO_ROOT/.env" 2>/dev/null | head -1 | cut -d= -f2- | sed "s/^['\"]//;s/['\"]$//")
  [[ -n "$_val" ]] && REDIS_PASS="$_val"
fi

check() {
  local name="$1"
  local cmd="$2"
  printf "  %-28s " "$name"
  if bash -c "$cmd" &>/dev/null 2>&1; then
    echo -e "${GREEN}OK${NC}"
  else
    echo -e "${RED}FAIL${NC}"
    PASS=false
  fi
}

echo ""
echo -e "${BOLD}FinStreamAI — Health Check${NC}"
echo "=========================================="

echo ""
echo "Infrastructure:"
check "Kafka"               "docker exec finstreami-kafka kafka-broker-api-versions --bootstrap-server localhost:9092"
check "Schema Registry"     "curl -sf http://localhost:8081/subjects"
check "PostgreSQL"          "docker exec finstreami-postgres pg_isready -U finstreami -d finstreami"
check "TimescaleDB"         "docker exec finstreami-timescaledb pg_isready -U timescale -d timescaledb"
check "Redis"               "docker exec finstreami-redis redis-cli -a '${REDIS_PASS}' ping 2>/dev/null | grep -q PONG"
check "Neo4j"               "curl -sf http://localhost:7474"
check "MinIO"               "curl -sf http://localhost:9000/minio/health/live"
check "Prometheus"          "curl -sf http://localhost:9090/-/healthy"
check "Grafana"             "curl -sf http://localhost:3001/api/health"
check "Jaeger"              "curl -sf http://localhost:16686"

echo ""
echo "Application:"
check "api-services"        "curl -sf http://localhost:8000/live"
check "Ingest metrics"      "curl -sf http://localhost:8001/metrics"
check "Stream metrics"      "curl -sf http://localhost:8002/metrics"

echo ""
if $PASS; then
  echo -e "${GREEN}All services healthy.${NC}"
  exit 0
else
  echo -e "${RED}One or more services failed. Run 'make logs-api / logs-ingest / logs-stream' to diagnose.${NC}"
  exit 1
fi
