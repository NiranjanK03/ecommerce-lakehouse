#!/usr/bin/env bash
# Pull Bitnami Helm chart images and push them to the local k3d registry.
#
# Why this script exists:
#   Bitnami moved all images off Docker Hub. k3d container nodes can't resolve
#   registry.bitnami.com (DNS limitation inside Docker's bridge network).
#   This script runs on the Mac host (where DNS works), pulls from
#   registry.bitnami.com, and pushes to registry.localhost:5001 — the local
#   registry that k3d is already configured to use.
#
#   After this runs, all Helm values files use:
#     global.imageRegistry: registry.localhost:5001
#   so pods never need to reach any external registry.
#
# Run once before make deploy-infra.
# Re-run after make clean to repopulate the local registry.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# Docker push from the Mac uses localhost:5001 (hostPort).
# k3d containerd pulls using registry.localhost:5001 (the k3d-configured mirror).
# Both point to the same registry container — image paths match across hostnames.
LOCAL_PUSH="localhost:5001"
GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'

log()  { echo -e "${GREEN}[pull-images]${NC} $*"; }
warn() { echo -e "${YELLOW}[pull-images]${NC} $*"; }

# Verify the local registry is reachable
check_registry() {
    if ! curl -sf "http://${LOCAL_PUSH}/v2/" >/dev/null 2>&1; then
        echo "ERROR: Local registry not reachable at http://${LOCAL_PUSH}"
        echo "Make sure the cluster is up: make cluster-up"
        exit 1
    fi
    log "Local registry reachable at ${LOCAL_PUSH}"
}

# Pull from registry.bitnami.com, tag for local registry, push.
# img_path: e.g. bitnami/minio:2025.7.23-debian-12-r3
pull_and_push() {
    local img_path="$1"
    local src="registry.bitnami.com/${img_path}"
    local dst="${LOCAL_PUSH}/${img_path}"

    if curl -sf "http://${LOCAL_PUSH}/v2/${img_path%%:*}/manifests/${img_path##*:}" >/dev/null 2>&1; then
        warn "Already in local registry: ${img_path} — skipping"
        return
    fi

    log "Pulling ${src}..."
    docker pull "${src}"
    docker tag  "${src}" "${dst}"
    log "Pushing to local registry: ${img_path}"
    docker push "${dst}"
}

# Extract bitnami image paths from a helm chart template output.
bitnami_images_for() {
    local chart="$1" version="$2" values_file="$3"
    helm template _test "${chart}" \
        --version "${version}" \
        --values "${values_file}" 2>/dev/null \
        | grep -E '^\s+image:' \
        | sed 's/.*image: //;s/"//g' \
        | grep -E '(docker\.io|registry.*docker\.io|registry-1\.docker\.io)/bitnami/' \
        | sed 's|.*docker\.io/||' \
        | sort -u
}

check_registry

log "=== Phase 1: MinIO (chart 17.0.21) ==="
while IFS= read -r img; do
    pull_and_push "${img}"
done < <(bitnami_images_for bitnami/minio 17.0.21 \
    "${ROOT}/infrastructure/k8s/helm/values/minio.yaml")

log "=== Phase 2: Kafka (chart 32.4.3) ==="
while IFS= read -r img; do
    pull_and_push "${img}"
done < <(bitnami_images_for bitnami/kafka 32.4.3 \
    "${ROOT}/infrastructure/k8s/helm/values/kafka.yaml")

log "=== Phase 3: PostgreSQL (chart 18.6.6) ==="
# The chart uses tag: latest — pull latest and also tag it explicitly
# so make clean + re-run doesn't silently get a different version.
PSQL_LATEST_TAG=$(docker pull registry.bitnami.com/bitnami/postgresql:latest 2>&1 \
    | grep "Digest:" | awk '{print $2}' | cut -d: -f2 | head -c 12 || echo "latest")
docker tag registry.bitnami.com/bitnami/postgresql:latest \
    "${LOCAL_PUSH}/bitnami/postgresql:latest"
docker push "${LOCAL_PUSH}/bitnami/postgresql:latest"
log "PostgreSQL pushed (digest prefix: ${PSQL_LATEST_TAG})"

while IFS= read -r img; do
    pull_and_push "${img}"
done < <(bitnami_images_for bitnami/postgresql 18.6.6 \
    "${ROOT}/infrastructure/k8s/helm/values/postgres.yaml" \
    | grep -v 'postgresql:latest')  # already handled above

log ""
log "All images are in the local registry."
log "Next: make deploy-infra"
