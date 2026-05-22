#!/usr/bin/env bash
# Stop all FinStreamAI services: app processes and Docker infrastructure.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PID_DIR="$REPO_ROOT/logs/pids"

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

info() { echo -e "${GREEN}[stop]${NC} $*"; }
warn() { echo -e "${YELLOW}[stop]${NC} $*"; }

stop_process() {
  local name="$1"
  local pid_file="$PID_DIR/$name.pid"

  if [[ ! -f "$pid_file" ]]; then
    warn "$name — no PID file found (already stopped?)"
    return
  fi

  local pid
  pid=$(cat "$pid_file")

  if kill -0 "$pid" 2>/dev/null; then
    info "Stopping $name (PID $pid)..."
    kill "$pid"
    local waited=0
    while kill -0 "$pid" 2>/dev/null && [[ $waited -lt 10 ]]; do
      sleep 0.5
      waited=$((waited + 1))
    done
    if kill -0 "$pid" 2>/dev/null; then
      warn "$name did not stop gracefully — sending SIGKILL"
      kill -9 "$pid" 2>/dev/null || true
    fi
  else
    warn "$name — PID $pid not running (stale PID file)"
  fi

  rm -f "$pid_file"
}

info "Stopping application processes..."
stop_process "stream"
stop_process "ingest"
stop_process "api"

info "Stopping Docker infrastructure..."
DOCKER_COMPOSE_CMD="docker compose"
if ! docker compose version &>/dev/null 2>&1; then
  DOCKER_COMPOSE_CMD="docker-compose"
fi
cd "$REPO_ROOT"
$DOCKER_COMPOSE_CMD down

info "All services stopped."
