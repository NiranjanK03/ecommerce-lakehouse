#!/usr/bin/env bash
# Start/stop port-forwards for local development.
#
# Usage:
#   bash scripts/port-forward.sh          # start all
#   bash scripts/port-forward.sh start    # start all
#   bash scripts/port-forward.sh stop     # stop all
#
# PIDs are tracked in .pf-pids/ so stop is clean.
# Port map: see PROJECT_MAP.md

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PID_DIR="$ROOT/.pf-pids"
ACTION="${1:-start}"

start_forward() {
    local name=$1 namespace=$2 svc=$3 local_port=$4 remote_port=$5
    local pid_file="$PID_DIR/${name}.pid"

    if [ -f "$pid_file" ] && kill -0 "$(cat "$pid_file")" 2>/dev/null; then
        echo "  [skip] $name already running (PID $(cat "$pid_file"))"
        return
    fi

    kubectl port-forward "svc/$svc" "${local_port}:${remote_port}" \
        -n "$namespace" &>/tmp/pf-${name}.log &
    echo $! > "$pid_file"
    echo "  [ok]   $name  localhost:${local_port} → ${svc}:${remote_port}"
}

start_all() {
    mkdir -p "$PID_DIR"
    echo "Starting port-forwards..."
    echo ""
    # Core infrastructure
    start_forward "minio-api"      infrastructure  minio                  9000  9000
    start_forward "minio-console"  infrastructure  minio                  9001  9001
    start_forward "postgres"       infrastructure  postgres-postgresql    5432  5432
    start_forward "iceberg"        infrastructure  iceberg-rest-catalog   8181  8181
    # Streaming / CDC
    start_forward "kafka"           streaming       kafka                  9092  9092
    start_forward "debezium"        streaming       debezium-connect       8083  8083
    # Orchestration
    start_forward "airflow"         orchestration   airflow-webserver      8080  8080
    # Serving — StarRocks + Trino
    start_forward "starrocks-mysql"  serving  kube-starrocks-fe  9030  9030
    start_forward "starrocks-ui"     serving  kube-starrocks-fe  8086  8080
    start_forward "trino"            serving  trino              8085  8080

    echo ""
    echo "Services:"
    echo "  MinIO Console  →  http://localhost:9001   (minioadmin / minioadmin)"
    echo "  MinIO S3 API   →  http://localhost:9000"
    echo "  PostgreSQL     →  localhost:5432          (olist_user / olist_pass / olist)"
    echo "  Iceberg REST   →  http://localhost:8181/v1/config"
    echo "  Kafka          →  localhost:9092"
    echo "  Debezium REST  →  http://localhost:8083/connectors"
    echo "  Airflow UI     →  http://localhost:8080  (admin / lakehouse-local)"
    echo "  StarRocks MySQL→  mysql -h 127.0.0.1 -P 9030 -u root --password=''"
    echo "  StarRocks UI   →  http://localhost:8086"
    echo "  Trino UI       →  http://localhost:8085"
    echo ""
    echo "Logs:  /tmp/pf-*.log"
    echo "Stop:  bash scripts/port-forward.sh stop"
}

stop_all() {
    echo "Stopping port-forwards..."
    if [ -d "$PID_DIR" ]; then
        for f in "$PID_DIR"/*.pid; do
            [ -f "$f" ] || continue
            pid=$(cat "$f")
            name=$(basename "$f" .pid)
            if kill -0 "$pid" 2>/dev/null; then
                kill "$pid" && echo "  [stopped] $name (PID $pid)"
            fi
            rm -f "$f"
        done
    fi
    echo "Done."
}

case "$ACTION" in
    start) start_all ;;
    stop)  stop_all  ;;
    *)     echo "Usage: $0 [start|stop]"; exit 1 ;;
esac
