# Runbook: Spark on Kubernetes

## Components

| Resource | Namespace | Purpose |
|----------|-----------|---------|
| spark-operator Deployment | processing | Watches SparkApplication CRDs, manages pod lifecycle |
| bronze-cdc-ingestion SparkApplication | processing | Long-running Bronze streaming job |
| silver-ingest SparkApplication | processing | Batch Silver MERGE INTO  |

## Deploy

```bash
# 1. Deploy the operator (installs CRDs + controller)
make deploy-spark-operator

# 2. Build and push the Bronze Docker image
make build-bronze-image

# 3. Initialise Bronze Iceberg tables (run once)
make init-bronze-tables

# 4. Start the Bronze streaming job
make start-bronze
```

## Monitor

```bash
# List all Spark jobs
kubectl get sparkapplication -n processing

# Watch the Bronze driver logs (streaming, follows indefinitely)
kubectl logs -n processing \
  $(kubectl get pods -n processing -l spark-role=driver -o name | grep bronze) \
  -f

# Check executor pods
kubectl get pods -n processing -l spark-role=executor

# OTel: query Bronze E2E latency in Prometheus (after port-forward on 9090)
curl -s 'http://localhost:9090/api/v1/query?query=histogram_quantile(0.95,sum(rate(pipeline_bronze_e2e_latency_seconds_bucket[5m]))by(le))' \
  | jq '.data.result[0].value[1]'
```

## Restart the Bronze job

The operator restarts the driver automatically on crash. To force a manual restart:
```bash
kubectl delete sparkapplication bronze-cdc-ingestion -n processing
kubectl apply -f pipelines/spark/applications/bronze-streaming.yaml
```

## Build the Spark image

```bash
# Build from repo root
docker build \
  -t registry.localhost:5001/spark-bronze:latest \
  -f pipelines/spark/docker/Dockerfile \
  pipelines/spark/

# Push to local registry (accessible inside k3d)
docker push registry.localhost:5001/spark-bronze:latest
```

## Common issues

**SparkApplication stuck in PENDING:**
```bash
kubectl describe sparkapplication bronze-cdc-ingestion -n processing
# Look for: admission webhook errors → check spark-operator webhook is healthy
kubectl get pods -n processing -l app.kubernetes.io/name=spark-operator
```

**Driver pod CrashLoopBackOff:**
```bash
kubectl logs -n processing \
  $(kubectl get pods -n processing -l spark-role=driver -o name | head -1) \
  --previous
# Common causes:
# - Image not found: run make build-bronze-image
# - OTel collector unreachable: check observability namespace
# - Kafka unreachable: check streaming namespace
# - Iceberg catalog unreachable: check infrastructure namespace
```

**Checkpoint corruption (job fails to resume after restart):**
```bash
# Clear checkpoint and restart from beginning
kubectl exec -n infrastructure deploy/minio -- \
  mc rm --recursive --force local/lakehouse-checkpoints/bronze/
kubectl delete sparkapplication bronze-cdc-ingestion -n processing
kubectl apply -f pipelines/spark/applications/bronze-streaming.yaml
```
Note: clearing the checkpoint means Bronze will re-read from the earliest Kafka offset. Iceberg's append-only model means no duplicate Bronze rows — re-ingested events are additional rows.

**Executor pods not scaling up (dynamic allocation not working):**
Verify `spark.dynamicAllocation.shuffleTracking.enabled = true` is in the SparkApplication CRD. Without this, Spark cannot track shuffle data and won't release executors safely.
