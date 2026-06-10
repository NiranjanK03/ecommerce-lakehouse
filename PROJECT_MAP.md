# Project Map

Quick reference for navigating the repo and running the stack.

---

## Where things live

| Directory | What's in it |
|-----------|-------------|
| `infrastructure/k8s/cluster/` | k3d cluster definition (1 file) |
| `infrastructure/k8s/namespaces.yaml` | 6 Kubernetes namespaces |
| `infrastructure/k8s/helm/values/` | Helm values for every service — one file per component |
| `infrastructure/k8s/helm/charts/` | Custom Kubernetes manifests (Iceberg catalog, Debezium, postgres_exporter) |
| `infrastructure/k8s/configmaps/` | Grafana dashboard JSON |
| `pipelines/spark/jobs/` | PySpark application code |
| `pipelines/spark/applications/` | SparkApplication CRDs — what Kubernetes actually runs |
| `pipelines/spark/docker/` | Dockerfile for the Spark image |
| `pipelines/airflow/dags/` | Airflow DAG definitions |
| `pipelines/airflow/plugins/` | Custom Airflow sensors |
| `pipelines/debezium/connectors/` | Debezium connector JSON configs |
| `pipelines/debezium/scripts/` | Connector registration script |
| `storage/adapters/` | MinIO / S3 / ADLS storage adapters (Python) |
| `transform/dbt/` | dbt-StarRocks Gold models (coming) |
| `agent/` | FastAPI + Claude text-to-SQL agent (coming) |
| `config/` | Environment configs — `local.yaml`, `aws.yaml`, `azure.yaml` |
| `docs/architecture/decisions/` | Architecture Decision Records (ADR-001 → ADR-006) |
| `docs/runbooks/` | How to deploy and debug each component |
| `scripts/` | Bash utilities: cluster setup, port-forward, data loading |

---

## Getting the cluster running

**One-time setup:**

```bash
# 1. Create cluster + local Docker registry
make cluster-up

# 2. Deploy core storage layer (MinIO → Iceberg catalog → PostgreSQL → Kafka)
make deploy-infra

# 3. Open service tunnels
make port-forward
```

**CDC + streaming pipeline:**

```bash
# 4. Build the Spark Docker image and push to the local registry
make build-bronze-image

# 5. Deploy Spark operator, Debezium, and OTel Collector
make deploy-streaming

# 6. Create Bronze Iceberg tables (run once)
make init-bronze-tables

# 7. Register the Debezium connector → starts reading PostgreSQL WAL
make register-connector

# 8. Start the Bronze streaming Spark job
make start-bronze
```

**Orchestration + Silver:**

```bash
# 9. Deploy Airflow
make deploy-orchestration

# 10. Create Silver Iceberg tables (run once)
make init-silver-tables

# 11. Trigger the Silver → Gold pipeline (self-retriggering loop)
make trigger-silver-dag
```

**Full observability (optional, deploy any time after streaming):**

```bash
make deploy-observability
```

---

## Port map (after `make port-forward`)

| Service | URL / Address | Credentials |
|---------|--------------|-------------|
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |
| MinIO S3 API | http://localhost:9000 | — |
| PostgreSQL | localhost:5432 | olist_user / olist_pass / olist |
| Iceberg REST Catalog | http://localhost:8181/v1/config | — |
| Kafka | localhost:9092 | — |
| Debezium REST API | http://localhost:8083/connectors | — |
| Airflow UI | http://localhost:8080 | admin / lakehouse-local |
| Grafana | http://localhost:3000 | admin / lakehouse-local |
| Prometheus | http://localhost:9090 | — |

---

## Kubernetes namespace map

| Namespace | What lives there |
|-----------|-----------------|
| `infrastructure` | MinIO, PostgreSQL, Iceberg REST Catalog, postgres_exporter |
| `streaming` | Kafka, Debezium Kafka Connect |
| `processing` | Spark operator, SparkApplication jobs |
| `orchestration` | Airflow |
| `serving` | StarRocks FE + CN, FastAPI agent |
| `observability` | OTel Collector, Prometheus, Grafana, Tempo |

---

## Debugging

```bash
# See all pods across namespaces
make status

# Follow logs
make logs-kafka
make logs-minio
make logs-catalog

# Check Debezium connector health
make connector-status

# Check Airflow DAG runs
make dag-status

# Bronze streaming job logs
kubectl logs -n processing \
  $(kubectl get pods -n processing -l spark-role=driver -o name | head -1) -f

# Iceberg tables in the catalog
curl -s http://localhost:8181/v1/namespaces/bronze/tables | python3 -m json.tool
```

---

## Reset everything

```bash
make clean   # deletes cluster + all data in /tmp/k3dvol
```
