#!/usr/bin/env bash
# Completely destroy the local lakehouse environment.
# WARNING: All persistent data in /tmp/k3dvol is deleted.

set -euo pipefail

read -rp "This will delete the k3d cluster and all data. Continue? [y/N] " confirm
[[ "$confirm" =~ ^[Yy]$ ]] || { echo "Aborted."; exit 0; }

echo "==> Stopping port-forwards..."
bash "$(dirname "$0")/port-forward.sh" stop 2>/dev/null || true

echo "==> Deleting k3d cluster 'lakehouse'..."
k3d cluster delete lakehouse 2>/dev/null || echo "  (cluster not found — already deleted)"

echo "==> Removing local volume data..."
rm -rf /tmp/k3dvol

echo "==> Done. All local lakehouse data removed."
