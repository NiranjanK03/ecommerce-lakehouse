# Runbook: Trino (benchmark comparison — temporary)

## What it does
Trino is deployed temporarily alongside StarRocks to produce the query performance comparison in ADR-007. It reads the same Silver Iceberg tables via the same REST catalog and MinIO. Once benchmark results are documented, run `make remove-trino` to free cluster resources.

## Deploy

```bash
make deploy-trino

# Verify
kubectl get pods -n serving -l "app.kubernetes.io/name=trino"
# Expected: trino-coordinator-xxx  1/1  Running
#           trino-worker-xxx       1/1  Running
```

## Access

```bash
make port-forward    # opens localhost:8085 → Trino coordinator

# Trino Web UI
open http://localhost:8085

# Trino CLI
trino --server http://localhost:8085 \
      --catalog lakehouse \
      --schema silver
```

## Verify Iceberg connector

```sql
-- From Trino CLI
SHOW CATALOGS;
-- Expected: lakehouse, system, tpch

SHOW SCHEMAS IN lakehouse;
-- Expected: bronze, silver, gold (if Silver tables exist)

SELECT COUNT(*) FROM lakehouse.silver.orders;
-- Returns row count if connector + MinIO are working
```

## Run benchmark queries manually

```bash
# From Trino CLI (after port-forward)
trino --server http://localhost:8085 \
      --catalog lakehouse \
      --schema silver \
      --file benchmarks/queries/q01_daily_revenue.sql
```

## Run full benchmark comparison

```bash
# Runs all 5 queries × 10 timed runs on both Trino and StarRocks
make run-benchmarks

# Then generate the report
make benchmark-report
# Report written to benchmarks/results/YYYY-MM-DD.md
```

## Remove Trino (after benchmarks complete)

```bash
make remove-trino
# Removes the Helm release from the serving namespace
```

## Common errors

**Iceberg REST catalog unreachable (ConnectException)**
Verify Iceberg REST catalog is running: `kubectl get pods -n infrastructure -l app=iceberg-rest-catalog`. Trino connects to the catalog in-cluster via `http://iceberg-rest-catalog.infrastructure.svc.cluster.local:8181`. No port-forward needed for this connection.

**MinIO 403 errors in Trino query logs**
Verify `s3.path-style-access=true` in `trino.yaml` under `additionalCatalogs.lakehouse`. Trino uses virtual-hosted style by default; MinIO requires path style.

**Worker pod OOMKilled during Q05 (cohort retention)**
Q05 is the most memory-intensive query. Increase `worker.resources.limits.memory` in `trino.yaml` and run `make deploy-trino` to apply (Helm upgrade is idempotent).

## Logs

```bash
kubectl logs -n serving -l "component=coordinator" --follow
kubectl logs -n serving -l "component=worker"      --follow
```
