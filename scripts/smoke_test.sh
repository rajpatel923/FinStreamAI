#!/usr/bin/env bash
# End-to-end smoke test: confirms every service is up and data is actually
# flowing through the pipeline (Kafka -> stream-processing -> ai-services ->
# data-lake / TimescaleDB -> agent-service -> api-gateway), not just that
# containers are running.
set -uo pipefail

PASS=0
FAIL=0

ok()   { printf "  \033[32mPASS\033[0m  %s\n" "$1"; PASS=$((PASS+1)); }
bad()  { printf "  \033[31mFAIL\033[0m  %s\n" "$1"; FAIL=$((FAIL+1)); }

echo "== 1. Container status =="
docker compose ps --format '{{.Name}}\t{{.Status}}' | while read -r name status; do
    if echo "$status" | grep -qi "unhealthy\|restarting\|exited"; then
        printf "  \033[31m%-32s %s\033[0m\n" "$name" "$status"
    else
        printf "  %-32s %s\n" "$name" "$status"
    fi
done

echo
echo "== 2. HTTP health endpoints =="
for svc in "api-gateway:8005" "agent-service:8006" "ai-services:8003" "data-lake:8004"; do
    name="${svc%%:*}"; port="${svc##*:}"
    code=$(curl -s -o /dev/null -w "%{http_code}" --max-time 3 "http://localhost:${port}/health")
    if [ "$code" = "200" ]; then ok "$name /health -> 200"; else bad "$name /health -> $code"; fi
done

echo
echo "== 3. Kafka topics carrying live traffic (5s sample each) =="
for topic in market.ticks.raw market.ticks.clean news.articles.raw events.extracted \
             alerts.anomalies predictions.signals agent.recommendations watchlist.signals; do
    n=$(timeout 6 docker compose exec -T kafka kafka-console-consumer \
        --bootstrap-server localhost:9092 --topic "$topic" \
        --max-messages 1 --timeout-ms 5000 2>/dev/null | grep -c '.')
    if [ "$n" -ge 1 ]; then ok "$topic — message seen"; else bad "$topic — no message in 5s (may just be a slow topic)"; fi
done

echo
echo "== 4. Bronze Delta tables (MinIO) row counts =="
# Run one table per python invocation and take only the last stdout line —
# deltalake's Rust layer sometimes dumps a per-file debug listing to stdout
# during to_pandas(), which would otherwise corrupt line-based parsing here.
for t in market_tick news_article social_post event; do
    result=$(docker compose exec -T data-lake python3 -c "
from deltalake import DeltaTable
opts = {'endpoint_url': 'http://minio:9000', 'aws_access_key_id': 'minioadmin',
        'aws_secret_access_key': 'minioadmin123', 'aws_region': 'us-east-1', 'allow_http': 'true'}
try:
    dt = DeltaTable('s3://finstreami-datalake/bronze/$t', storage_options=opts)
    print('RESULT', len(dt.to_pandas()))
except Exception as e:
    print('RESULT ERROR:' + str(e).splitlines()[0][:80])
" 2>/dev/null | grep '^RESULT' | tail -1 | sed 's/^RESULT //')
    if [[ "$result" =~ ^[0-9]+$ ]] && [ "$result" -gt 0 ]; then
        ok "bronze/$t — $result rows"
    else
        bad "bronze/$t — $result"
    fi
done

echo
echo "== 5. TimescaleDB row counts =="
# market_ticks/sentiment_scores are legacy tables nothing currently writes to —
# only trading_signals and technical_indicators have an active writer
# (stream-processing/src/sinks/timescaledb_sink.py), so only check those.
for table in trading_signals technical_indicators; do
    count=$(docker compose exec -T timescaledb psql -U timescale -d timescaledb -tAc "SELECT count(*) FROM $table;" 2>/dev/null | tr -d '[:space:]')
    if [[ "$count" =~ ^[0-9]+$ ]] && [ "$count" -gt 0 ]; then
        ok "$table — $count rows"
    else
        bad "$table — $count"
    fi
done

echo
echo "== 6. LLM provider wired and reachable =="
for svc in ai-services agent-service; do
    provider=$(docker compose exec -T "$svc" printenv LLM_PROVIDER 2>/dev/null | tr -d '[:space:]')
    if [ -n "$provider" ]; then ok "$svc LLM_PROVIDER=$provider"; else bad "$svc — LLM_PROVIDER not set"; fi
done
code=$(docker compose exec -T ai-services python3 -c "
import httpx
try:
    r = httpx.get('http://host.docker.internal:11434/v1/models', timeout=3)
    print(r.status_code)
except Exception as e:
    print('ERR')
" 2>/dev/null)
if [ "$code" = "200" ]; then ok "llama.cpp server reachable from containers"; else bad "llama.cpp server unreachable ($code)"; fi

echo
echo "=================================="
echo "  $PASS passed, $FAIL failed"
echo "=================================="
