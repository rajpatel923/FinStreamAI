#!/usr/bin/env bash
set -euo pipefail

for topic in "market.ticks.raw:50" "market.ticks.clean:50" "market.bars.1min:20" "market.bars.5min:10" "news.articles.raw:10" "news.articles.scored:10" "social.posts.raw:15" "social.sentiment:15" "events.extracted:5" "technical.indicators:20" "predictions.signals:10" "alerts.anomalies:5"; do
  name="${topic%:*}"
  parts="${topic#*:}"
  docker exec finstreami-kafka kafka-topics \
    --bootstrap-server localhost:9092 \
    --create --if-not-exists \
    --topic "$name" --partitions "$parts" --replication-factor 1
done