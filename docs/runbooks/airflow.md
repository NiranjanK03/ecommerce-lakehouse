# Runbook: Apache Airflow

## What it does
Airflow orchestrates the Silver → Gold pipeline. It runs the IcebergSnapshotSensor to detect new Bronze data, triggers the Silver SparkApplication batch job, and refreshes the StarRocks Gold materialized views. The pipeline is self-retriggering — it loops continuously without a fixed schedule.

## Deploy

```bash
# Prerequisites: PostgreSQL must be running, airflow database must exist
kubectl port-forward svc/postgres-postgresql 5432:5432 -n infrastructure &
psql -h localhost -U olist_user -d postgres \
  -c "CREATE DATABASE airflow;"

make deploy-airflow

# Verify all pods are ready (takes ~3 minutes)
kubectl get pods -n orchestration
# Expected:
#   airflow-webserver-XXX    1/1 Running
#   airflow-scheduler-XXX    1/1 Running
#   airflow-triggerer-XXX    1/1 Running
```

## Sync DAGs and plugins

```bash
# After any DAG or plugin change:
make sync-dags
make sync-plugins
```

## Access

```bash
kubectl port-forward svc/airflow-webserver 8080:8080 -n orchestration &
# Open: http://localhost:8080
# Login: admin / lakehouse-local
```

## Trigger the pipeline (first run)

```bash
make trigger-silver-dag
# or via CLI:
kubectl exec -n orchestration \
  $(kubectl get pods -n orchestration -l component=scheduler -o name | head -1) -- \
  airflow dags trigger silver_gold_pipeline
```

After the first manual trigger, the DAG self-retriggeres via `TriggerDagRunOperator`.

## Monitor

```bash
# List recent DAG runs
kubectl exec -n orchestration \
  $(kubectl get pods -n orchestration -l component=scheduler -o name | head -1) -- \
  airflow dags list-runs -d silver_gold_pipeline --limit 5

# Check IcebergSnapshotSensor variable (last seen snapshot)
kubectl exec -n orchestration \
  $(kubectl get pods -n orchestration -l component=scheduler -o name | head -1) -- \
  airflow variables get iceberg_last_snapshot_bronze_orders
```

## Common issues

**Webserver not reachable:**
```bash
kubectl logs -n orchestration deploy/airflow-webserver --tail=30
# If DB connection fails: verify airflow database exists in PostgreSQL
```

**SparkKubernetesOperator fails with "connection refused":**
```bash
# The operator uses kubernetes_default connection pointing to the in-cluster API
# Verify it's configured correctly:
kubectl exec -n orchestration \
  $(kubectl get pods -n orchestration -l component=scheduler -o name | head -1) -- \
  airflow connections get kubernetes_default
```

**IcebergSnapshotSensor always returns False:**
```bash
# Check the Bronze table has at least one snapshot
curl -s http://localhost:8181/v1/namespaces/bronze/tables/orders | \
  python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('metadata',{}).get('current-snapshot-id'))"
# If null: Bronze streaming job hasn't written any data yet
# Check: kubectl logs -n processing $(kubectl get pods -n processing -l spark-role=driver -o name) --tail=30
```

**DAG self-trigger loop running too fast (hammering the catalog):**
```bash
# Increase poke_interval in the DAG from 30 to 60 seconds
# Or add a minimum_wait between retriggers using TimeDeltaSensor
```
