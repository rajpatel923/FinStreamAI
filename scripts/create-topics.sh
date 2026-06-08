#!/usr/bin/env bash
# Create all 14 FinStreamAI Kafka topics. Idempotent (--if-not-exists).
set -euo pipefail

KAFKA_TOPICS=(
  "market.ticks.raw:50"
  "market.ticks.clean:50"
  "market.bars.1min:20"
  "market.bars.5min:10"
  "market.bars.15min:5"
  "market.bars.1hour:3"
  "technical.indicators:20"
  "news.articles.raw:10"
  "news.articles.scored:10"
  "social.posts.raw:15"
  "social.sentiment:15"
  "events.extracted:5"
  "alerts.anomalies:5"
  "predictions.signals:10"
  # Phase 7 — Agent service topics
  "watchlist.signals:5"
  "agent.recommendations:5"
  "trade.orders.submitted:5"
  "trade.orders.filled:5"
)

echo "Creating Kafka topics..."
for entry in "${KAFKA_TOPICS[@]}"; do
  topic="${entry%%:*}"
  partitions="${entry##*:}"
  printf "  %-35s %s partitions\n" "$topic" "$partitions"
  docker exec finstreami-kafka kafka-topics \
    --bootstrap-server localhost:9092 \
    --create \
    --if-not-exists \
    --topic "$topic" \
    --partitions "$partitions" \
    --replication-factor 1 2>/dev/null || true
done
echo "All Kafka topics created."
