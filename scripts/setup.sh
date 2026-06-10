#!/usr/bin/env bash
# Full infrastructure setup — idempotent.
# Matches the installation order in CLAUDE.md exactly:
#   1. k3d cluster
#   2. namespaces
#   3. MinIO
#   4. Iceberg REST Catalog
#   5. PostgreSQL
#   6. Kafka

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'

log()  { echo -e "${GREEN}[setup]${NC} $*"; }
warn() { echo -e "${YELLOW}[setup]${NC} $*"; }

check_prereqs() {
    log "Checking prerequisites..."
    local missing=()
    for cmd in k3d kubectl helm docker; do
        command -v "$cmd" &>/dev/null || missing+=("$cmd")
    done
    if [ ${#missing[@]} -ne 0 ]; then
        echo "Missing tools: ${missing[*]}"
        echo "Install with: brew install k3d kubectl helm"
        exit 1
    fi
    docker info &>/dev/null || { echo "Docker not running."; exit 1; }
    log "Prerequisites OK"
}

create_cluster() {
    if k3d cluster list 2>/dev/null | grep -q "lakehouse"; then
        warn "Cluster 'lakehouse' already exists — skipping"
        return
    fi
    log "Creating k3d cluster..."
    mkdir -p /tmp/k3dvol
    k3d cluster create --config "$ROOT/infrastructure/k8s/cluster/k3d-config.yaml"
    log "Cluster created"
}

add_helm_repos() {
    log "Adding Helm repos..."
    helm repo add bitnami https://charts.bitnami.com/bitnami 2>/dev/null || true
    helm repo update
}

apply_namespaces() {
    log "Applying namespaces..."
    kubectl apply -f "$ROOT/infrastructure/k8s/namespaces.yaml"
}

# Step 3: MinIO — first, everything else depends on it
deploy_minio() {
    log "Deploying MinIO (step 3 — storage layer)..."
    helm upgrade --install minio bitnami/minio \
        --namespace infrastructure \
        --version 14.6.16 \
        --values "$ROOT/infrastructure/k8s/helm/values/minio.yaml" \
        --wait --timeout 5m
    log "MinIO deployed"
}

# Step 4: Iceberg REST Catalog — needs MinIO warehouse bucket
deploy_catalog() {
    log "Deploying Iceberg REST Catalog (step 4)..."
    kubectl apply -f "$ROOT/infrastructure/k8s/helm/charts/iceberg-rest/deployment.yaml"
    kubectl rollout status deployment/iceberg-rest-catalog \
        -n infrastructure --timeout=3m
    log "Iceberg REST Catalog deployed"
}

# Step 5: PostgreSQL — CDC source
deploy_postgres() {
    log "Creating Postgres init-scripts ConfigMap (step 5)..."
    kubectl create configmap postgres-init-scripts \
        --from-file=01-schema.sql="$ROOT/infrastructure/k8s/helm/values/postgres/init-scripts/01-schema.sql" \
        --from-file=02-sample-data.sql="$ROOT/infrastructure/k8s/helm/values/postgres/init-scripts/02-sample-data.sql" \
        --namespace infrastructure \
        --dry-run=client -o yaml | kubectl apply -f -
    helm upgrade --install postgres bitnami/postgresql \
        --namespace infrastructure \
        --version 15.5.17 \
        --values "$ROOT/infrastructure/k8s/helm/values/postgres.yaml" \
        --wait --timeout 5m
    log "PostgreSQL deployed"
}

# Step 6: Kafka — must be up before Debezium
deploy_kafka() {
    log "Deploying Kafka (step 6)..."
    helm upgrade --install kafka bitnami/kafka \
        --namespace streaming \
        --version 26.11.4 \
        --values "$ROOT/infrastructure/k8s/helm/values/kafka.yaml" \
        --wait --timeout 5m
    log "Kafka deployed"
}

print_summary() {
    log ""
    log "====================================================="
    log " Core infrastructure deployed"
    log "====================================================="
    log " Run:  bash scripts/port-forward.sh"
    log " Then: make status"
    log ""
    log " MinIO Console  →  http://localhost:9001"
    log " Iceberg REST   →  http://localhost:8181/v1/config"
    log " PostgreSQL     →  localhost:5432 (olist_user/olist_pass)"
}

main() {
    check_prereqs
    create_cluster
    add_helm_repos
    apply_namespaces
    deploy_minio
    deploy_catalog
    deploy_postgres
    deploy_kafka
    print_summary
}

main "$@"
