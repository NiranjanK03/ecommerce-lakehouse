# Runbook: MinIO

## What it does
MinIO is the object storage layer for the entire lakehouse. Every Iceberg data file, Spark checkpoint, and dbt artifact lives in a MinIO bucket. It speaks the S3 API, so all tooling that talks to S3 (Spark, Trino, Iceberg catalog) works unchanged.

## Deploy

```bash
make deploy-minio
# or manually:
helm upgrade --install minio bitnami/minio \
  --namespace infrastructure \
  --version 14.6.16 \
  --values infrastructure/k8s/helm/values/minio.yaml \
  --wait --timeout 5m
```

## Access

```bash
# Start port-forward
kubectl port-forward svc/minio 9000:9000 9001:9001 -n infrastructure

# Web console: http://localhost:9001
# Credentials: minioadmin / minioadmin

# S3 API (boto3, mc, awscli):
export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin
aws --endpoint-url http://localhost:9000 s3 ls
```

## Verify buckets were created

```bash
kubectl exec -it deploy/minio -n infrastructure -- mc ls local/
```

Expected output:
```
[DATE]  lakehouse-bronze
[DATE]  lakehouse-checkpoints
[DATE]  lakehouse-gold
[DATE]  lakehouse-silver
[DATE]  lakehouse-warehouse
```

## Common issues

**Provisioning job never completed:**  
Check: `kubectl logs job/minio-provisioning -n infrastructure`  
The job runs once after first install. It creates buckets using the MinIO mc client. If MinIO wasn't ready when the job ran, delete the job and re-run the Helm upgrade.

**PVC stuck in Pending:**  
Check: `kubectl describe pvc -n infrastructure`  
The local-path provisioner creates the PV only after the pod is scheduled. If the node doesn't have capacity, the PVC stays Pending until space is freed.

**Connection refused from Spark pod:**  
Ensure Spark pod is in the `processing` namespace. The FQDN is:  
`http://minio.infrastructure.svc.cluster.local:9000`  
Not `localhost:9000` (that's only valid after port-forward from your laptop).
