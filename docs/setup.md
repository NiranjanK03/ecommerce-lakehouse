# Setup Guide

**Goal:** A running k3d cluster with MinIO, PostgreSQL, Kafka, and the Iceberg REST Catalog deployed and accessible via port-forward.

**Time estimate:** 45–60 minutes on first run (mostly waiting for Helm deployments).

---

## Prerequisites

```bash
# macOS
brew install k3d kubectl helm

# Verify versions
k3d version    # need >= 5.6.0
kubectl version --client --short
helm version   # need >= 3.14.0

# Docker Desktop
# Settings → Resources → Memory: set to at least 8 GB
# (12 GB recommended — Kafka + MinIO + Spark together peak at ~10 GB)
```

---

## Step 1 — Create the k3d cluster

```bash
make cluster-up
```

This runs:
```bash
mkdir -p /tmp/k3dvol
k3d cluster create --config infrastructure/k8s/cluster/k3d-config.yaml
```

The cluster has:
- 1 server node (control plane)
- 3 agent nodes (workloads)
- A local Docker registry at `registry.localhost:5001`
- Traefik and ServiceLB disabled (we use port-forward)

Verify:
```bash
kubectl get nodes
# NAME                     STATUS   ROLES                  AGE
# k3d-lakehouse-server-0   Ready    control-plane,master   60s
# k3d-lakehouse-agent-0    Ready    <none>                 58s
# k3d-lakehouse-agent-1    Ready    <none>                 58s
# k3d-lakehouse-agent-2    Ready    <none>                 58s
```

---

## Step 2 — Apply namespaces

```bash
kubectl apply -f infrastructure/k8s/namespaces.yaml
kubectl get namespaces
```

Expected namespaces: `infrastructure`, `streaming`, `processing`, `orchestration`, `serving`, `observability`.

---

## Step 3 — Deploy MinIO (storage layer — deploy first)

```bash
make deploy-minio
```

MinIO is deployed first because every other component writes to it. The `provisioning` job creates all five buckets automatically on first boot. Wait for it to complete:

```bash
kubectl get pods -n infrastructure
# minio-0                     1/1     Running     0
# minio-provisioning-xxxxx    0/1     Completed   0   ← this is good
```

---

## Step 4 — Deploy PostgreSQL

```bash
make deploy-postgres
```

This also creates the `postgres-init-scripts` ConfigMap from the SQL files in `infrastructure/k8s/helm/values/postgres/` and runs the schema + seed data on first boot.

Verify the schema was applied:
```bash
# After Step 7 (port-forward), run:
psql -h localhost -U olist_user -d olist -c '\dt'
```

---

## Step 5 — Deploy Iceberg REST Catalog

```bash
make deploy-catalog
```

The catalog connects to PostgreSQL on startup (JDBC) and to MinIO for file operations. It will stay in `Pending` readiness until both are reachable.

```bash
kubectl get pods -n infrastructure
# iceberg-rest-catalog-xxxxx   1/1     Running   0
```

---

## Step 6 — Deploy Kafka

```bash
make deploy-kafka
```

Uses Bitnami Kafka chart with KRaft (no ZooKeeper). Topic provisioning creates all seven Olist CDC topics automatically.

```bash
kubectl get pods -n streaming
# kafka-controller-0   1/1     Running   0
```

---

## Step 7 — Start port-forwards

```bash
make port-forward
# or:
bash scripts/port-forward.sh start
```

Services available after port-forward:

| Service | URL / Connection |
|---------|----------------|
| MinIO Console | http://localhost:9001 (minioadmin/minioadmin) |
| MinIO S3 API | http://localhost:9000 |
| PostgreSQL | localhost:5432 (olist_user/olist_pass/olist) |
| Iceberg REST | http://localhost:8181/v1/config |
| Kafka | localhost:9092 |

---

## Step 8 — Verify everything

```bash
make status
```

Check the Iceberg catalog endpoint:
```bash
curl http://localhost:8181/v1/config | python3 -m json.tool
```

Browse MinIO buckets at http://localhost:9001 — you should see five buckets.

---

## Load the full Olist dataset (optional but recommended)

1. Download from Kaggle: https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce
2. Extract CSVs to `data/seed/`
3. Run: `make load-olist-data`

The seed data (3 customers, 2 orders) loaded by `02-sample-data.sql` is sufficient for CDC testing.

---

## Troubleshooting

**Pod stuck in Pending — no PVC:**
```bash
kubectl describe pod <name> -n <namespace>
# Look for: "0/4 nodes are available: 4 Insufficient memory"
# Fix: increase Docker Desktop memory allocation
```

**Helm timeout on Kafka:**
```bash
kubectl get pods -n streaming
kubectl logs kafka-controller-0 -n streaming
# KRaft initialization takes ~45 seconds; increase --timeout if on a slow machine
```

**Port-forward dies after a few minutes:**  
This is a known k8s port-forward limitation — the connection drops if idle.  
Run `bash scripts/port-forward.sh stop && bash scripts/port-forward.sh start` to restart.

---

## What's next: CDC pipeline

- Deploy Debezium connector to capture Olist CDC changes from PostgreSQL into Kafka
- Write a Spark Structured Streaming job to read from Kafka and write Bronze Iceberg tables
