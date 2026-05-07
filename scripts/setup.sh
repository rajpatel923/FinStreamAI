#!/usr/bin/env bash
set -euo pipefail

TOPICS_ONLY=${1:-""}

KAFKA_TOPICS=(
  "market.ticks.raw:50"
  "market.ticks.clean:50"
  "market.bars.1min:20"
  "news.articles.raw:10"
  "news.articles.scored:10"
  "social.posts.raw:15"
  "social.sentiment:15"
  "events.extracted:5"
  "alerts.anomalies:5"
  "predictions.signals:10"
)

create_topics() {
  echo "Creating Kafka topics..."
  for entry in "${KAFKA_TOPICS[@]}"; do
    topic="${entry%%:*}"
    partitions="${entry##*:}"
    echo "  Creating topic: $topic ($partitions partitions)"
    docker exec finstreami-kafka kafka-topics \
      --bootstrap-server localhost:9092 \
      --create \
      --if-not-exists \
      --topic "$topic" \
      --partitions "$partitions" \
      --replication-factor 1 2>/dev/null || true
  done
  echo "Kafka topics created."
}

if [[ "$TOPICS_ONLY" == "--topics-only" ]]; then
  create_topics
  exit 0
fi

echo "============================================"
echo " FinStreamAI — Local Development Setup"
echo "============================================"

# Check prerequisites
echo "Checking prerequisites..."
for cmd in docker docker-compose python3 git; do
  if ! command -v $cmd &>/dev/null; then
    echo "ERROR: $cmd is required but not installed."
    exit 1
  fi
done

DOCKER_COMPOSE_CMD="docker compose"
if ! docker compose version &>/dev/null 2>&1; then
  DOCKER_COMPOSE_CMD="docker-compose"
fi

echo "Prerequisites OK."

# Copy .env if not present
if [[ ! -f .env ]]; then
  echo "Creating .env from .env.example..."
  cp .env.example .env
  echo "WARNING: .env created — fill in real credentials before running data producers."
fi

# Start infrastructure services
echo "Starting infrastructure services..."
$DOCKER_COMPOSE_CMD up -d zookeeper kafka schema-registry postgres timescaledb redis neo4j minio prometheus grafana jaeger

echo "Waiting for services to be healthy..."
sleep 15

# Wait for Kafka
echo "Waiting for Kafka..."
until docker exec finstreami-kafka kafka-broker-api-versions --bootstrap-server localhost:9092 &>/dev/null 2>&1; do
  echo "  Kafka not ready yet, waiting..."
  sleep 5
done
echo "Kafka is ready."

# Create topics
create_topics

echo ""
echo "============================================"
echo " Setup complete!"
echo "============================================"
echo ""
echo "Services:"
echo "  PostgreSQL:     localhost:5432"
echo "  TimescaleDB:    localhost:5433"
echo "  Kafka:          localhost:9092"
echo "  Schema Registry: http://localhost:8081"
echo "  Redis:          localhost:6379"
echo "  Neo4j:          http://localhost:7474"
echo "  MinIO:          http://localhost:9001"
echo "  Prometheus:     http://localhost:9090"
echo "  Grafana:        http://localhost:3001 (\${GRAFANA_ADMIN_USER:-admin}/\${GRAFANA_ADMIN_PASSWORD:-from .env})"
echo "  Jaeger:         http://localhost:16686"
echo ""
echo "Next steps:"
echo "  make api         # Start FastAPI locally"
echo "  make test        # Run tests"
echo ""
