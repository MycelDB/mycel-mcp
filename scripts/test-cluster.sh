#!/usr/bin/env bash
set -euo pipefail

# Start/stop a disposable three-node Mycel raft cluster for local MCP testing.
# Defaults intentionally create an admin principal:
#   username: admin
#   password: password
#
# Usage:
#   scripts/test-cluster.sh start   # default
#   scripts/test-cluster.sh status
#   scripts/test-cluster.sh logs [myceld-a|myceld-b|myceld-c]
#   scripts/test-cluster.sh stop
#   scripts/test-cluster.sh env

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
MYCEL_ROOT="${MYCEL_ROOT:-$(cd "$REPO_DIR/../mycel" && pwd)}"
MYCELDB_ROOT="$(cd "$MYCEL_ROOT/.." && pwd)"
COMPOSE_FILE="$MYCEL_ROOT/tests/compose/cluster/compose.yml"
WORK_DIR="${MYCEL_TEST_CLUSTER_DIR:-$REPO_DIR/.tmp/test-cluster}"
OVERRIDE_FILE="$WORK_DIR/compose.override.yml"
ENV_FILE="$WORK_DIR/mcp-env.sh"
PROJECT_NAME="${MYCEL_TEST_PROJECT_NAME:-mycel-mcp-test}"
IMAGE="${MYCEL_TEST_IMAGE:-local/mycel:mcp-test}"
ADMIN_USERNAME="${MYCEL_TEST_ADMIN_USERNAME:-admin}"
ADMIN_PASSWORD="${MYCEL_TEST_ADMIN_PASSWORD:-password}"
BACKEND_TOKEN="${MYCEL_TEST_BACKEND_TOKEN:-mycel-mcp-test-cluster-token}"
CLUSTER_NAME="${MYCEL_TEST_CLUSTER_NAME:-mycel-mcp-test}"
# Use high host ports by default so the test cluster does not collide with a
# normal local standalone myceld on 127.0.0.1:9091.
HOST_A="${MYCEL_TEST_HOST_A:-127.0.0.1:19091}"
HOST_B="${MYCEL_TEST_HOST_B:-127.0.0.1:19092}"
HOST_C="${MYCEL_TEST_HOST_C:-127.0.0.1:19093}"
RESET="${MYCEL_TEST_RESET:-true}"
SKIP_BUILD="${MYCEL_TEST_SKIP_BUILD:-false}"
WAIT_TIMEOUT_SECONDS="${MYCEL_TEST_WAIT_TIMEOUT_SECONDS:-240}"

usage() {
  cat <<EOF
Usage: $0 [start|stop|restart|status|logs|env]

Starts a disposable three-node Mycel raft cluster using Docker Compose.

Defaults:
  admin username: $ADMIN_USERNAME
  admin password: $ADMIN_PASSWORD
  node A host:   $HOST_A
  node B host:   $HOST_B
  node C host:   $HOST_C

Environment overrides:
  MYCEL_ROOT                         path to mycel checkout
  MYCEL_TEST_ADMIN_USERNAME          default: admin
  MYCEL_TEST_ADMIN_PASSWORD          default: password
  MYCEL_TEST_HOST_A/B/C              default: 127.0.0.1:19091/19092/19093
  MYCEL_TEST_RESET=false             preserve existing compose volumes on start
  MYCEL_TEST_SKIP_BUILD=true         skip docker image build
  MYCEL_TEST_IMAGE                   default: local/mycel:mcp-test
  MYCEL_TEST_PROJECT_NAME            default: mycel-mcp-test
EOF
}

compose() {
  if docker compose version >/dev/null 2>&1; then
    docker compose -p "$PROJECT_NAME" -f "$COMPOSE_FILE" -f "$OVERRIDE_FILE" "$@"
  elif command -v docker-compose >/dev/null 2>&1; then
    docker-compose -p "$PROJECT_NAME" -f "$COMPOSE_FILE" -f "$OVERRIDE_FILE" "$@"
  else
    echo "docker compose is required" >&2
    exit 1
  fi
}

host_port() {
  local addr="$1"
  echo "${addr##*:}"
}

write_override() {
  mkdir -p "$WORK_DIR"
  cat > "$OVERRIDE_FILE" <<EOF
services:
  myceld-a:
    ports:
      - "127.0.0.1:$(host_port "$HOST_A"):9091"
  myceld-b:
    ports:
      - "127.0.0.1:$(host_port "$HOST_B"):9091"
  myceld-c:
    ports:
      - "127.0.0.1:$(host_port "$HOST_C"):9091"
EOF
  cat > "$ENV_FILE" <<EOF
# Source this file when testing host-side clients against the test cluster.
export MYCELD_GRPC_ADDR="$HOST_A"
export MYCEL_USERNAME="$ADMIN_USERNAME"
export MYCEL_PASSWORD="$ADMIN_PASSWORD"
EOF
}

compose_env() {
  export MYCEL_IMAGE="$IMAGE"
  export MYCEL_PULL_POLICY="never"
  export MYCELD_BOOTSTRAP_ADMIN_USERNAME="$ADMIN_USERNAME"
  export MYCELD_BOOTSTRAP_ADMIN_PASSWORD="$ADMIN_PASSWORD"
  export MYCELD_CLUSTER_NAME="$CLUSTER_NAME"
  export MYCELD_CLUSTER_BACKEND_AUTH_TOKEN="$BACKEND_TOKEN"
}

build_image() {
  if [[ "$SKIP_BUILD" == "true" ]]; then
    echo "Skipping image build because MYCEL_TEST_SKIP_BUILD=true"
    return
  fi
  echo "Building $IMAGE from $MYCELDB_ROOT using $MYCEL_ROOT/Dockerfile"
  docker build -f "$MYCEL_ROOT/Dockerfile" -t "$IMAGE" "$MYCELDB_ROOT"
}

port_in_use() {
  local addr="$1"
  local host="${addr%:*}"
  local port="${addr##*:}"
  if command -v lsof >/dev/null 2>&1; then
    lsof -nP -iTCP:"$port" -sTCP:LISTEN 2>/dev/null | grep -q .
    return $?
  fi
  nc -z "$host" "$port" >/dev/null 2>&1
}

preflight_ports() {
  local used=()
  for addr in "$HOST_A" "$HOST_B" "$HOST_C"; do
    if port_in_use "$addr"; then
      used+=("$addr")
    fi
  done
  if [[ ${#used[@]} -gt 0 ]]; then
    echo "Refusing to start: host port(s) already in use: ${used[*]}" >&2
    echo "Stop the process using the port(s), or choose alternate ports, for example:" >&2
    echo "  MYCEL_TEST_HOST_A=127.0.0.1:19191 MYCEL_TEST_HOST_B=127.0.0.1:19192 MYCEL_TEST_HOST_C=127.0.0.1:19193 scripts/test-cluster.sh start" >&2
    return 1
  fi
}

wait_for_readiness() {
  local deadline=$((SECONDS + WAIT_TIMEOUT_SECONDS))
  local services=(myceld-a myceld-b myceld-c)
  echo "Waiting up to ${WAIT_TIMEOUT_SECONDS}s for cluster readiness..."
  while (( SECONDS < deadline )); do
    local all_ready=true
    for service in "${services[@]}"; do
      if ! compose exec -T "$service" mycel \
        --daemon-addr 127.0.0.1:9091 \
        --username "$ADMIN_USERNAME" \
        --password "$ADMIN_PASSWORD" \
        cluster readiness check >/dev/null 2>&1; then
        all_ready=false
        break
      fi
    done
    if [[ "$all_ready" == "true" ]]; then
      echo "Cluster is ready."
      return 0
    fi
    sleep 3
  done
  echo "Timed out waiting for cluster readiness." >&2
  echo "Current containers:" >&2
  compose ps >&2 || true
  echo "Recent node-a logs:" >&2
  compose logs --tail=80 myceld-a >&2 || true
  return 1
}

start_cluster() {
  if [[ ! -f "$COMPOSE_FILE" ]]; then
    echo "compose fixture not found: $COMPOSE_FILE" >&2
    exit 1
  fi
  write_override
  compose_env
  build_image
  if [[ "$RESET" == "true" ]]; then
    echo "Resetting compose project and volumes: $PROJECT_NAME"
    compose down -v --remove-orphans >/dev/null 2>&1 || true
  fi
  preflight_ports
  echo "Starting compose project: $PROJECT_NAME"
  compose up -d
  wait_for_readiness
  cat <<EOF

Three-node Mycel test cluster is ready.

Admin credentials:
  username: $ADMIN_USERNAME
  password: $ADMIN_PASSWORD

Host gRPC endpoints:
  node A: $HOST_A
  node B: $HOST_B
  node C: $HOST_C

Host-side test env:
  source "$ENV_FILE"

Example checks:
  docker compose -p "$PROJECT_NAME" -f "$COMPOSE_FILE" -f "$OVERRIDE_FILE" ps
  MYCELD_GRPC_ADDR="$HOST_A" MYCEL_USERNAME="$ADMIN_USERNAME" MYCEL_PASSWORD="$ADMIN_PASSWORD" bin/mycel-mcp doctor
  bin/mycel-mcp login --daemon-addr "$HOST_A" --username "$ADMIN_USERNAME" --password "$ADMIN_PASSWORD"
EOF
}

stop_cluster() {
  write_override
  compose_env
  compose down -v --remove-orphans
}

status_cluster() {
  write_override
  compose_env
  compose ps
  echo
  compose exec -T myceld-a mycel \
    --daemon-addr 127.0.0.1:9091 \
    --username "$ADMIN_USERNAME" \
    --password "$ADMIN_PASSWORD" \
    --output json cluster health || true
}

logs_cluster() {
  write_override
  compose_env
  if [[ $# -gt 0 ]]; then
    compose logs -f "$1"
  else
    compose logs -f myceld-a myceld-b myceld-c
  fi
}

print_env() {
  write_override
  cat "$ENV_FILE"
}

cmd="${1:-start}"
case "$cmd" in
  start)
    start_cluster
    ;;
  stop)
    stop_cluster
    ;;
  restart)
    stop_cluster || true
    start_cluster
    ;;
  status)
    status_cluster
    ;;
  logs)
    shift || true
    logs_cluster "$@"
    ;;
  env)
    print_env
    ;;
  -h|--help|help)
    usage
    ;;
  *)
    usage >&2
    exit 1
    ;;
esac
