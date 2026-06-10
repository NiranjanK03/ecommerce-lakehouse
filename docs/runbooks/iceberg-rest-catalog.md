# Runbook: Iceberg REST Catalog

## What it does
The Iceberg REST Catalog tracks all Iceberg tables — their schemas, snapshots, partition specs, and data file locations. It is the "phone book" that Spark and Trino call to find where a table's data files live in MinIO.

It uses PostgreSQL as its persistent backend (the same instance as the Olist source data, but in a separate schema). Data files themselves remain in the `lakehouse-warehouse` MinIO bucket.

## Deploy

```bash
make deploy-catalog
# or manually:
kubectl apply -f infrastructure/k8s/helm/charts/iceberg-rest/deployment.yaml
kubectl rollout status deployment/iceberg-rest-catalog -n infrastructure --timeout=3m
```

**Deploy order dependency:** MinIO and PostgreSQL must be running first.

## Access

```bash
# Start port-forward
kubectl port-forward svc/iceberg-rest-catalog 8181:8181 -n infrastructure

# Verify the catalog config endpoint
curl http://localhost:8181/v1/config
# Expected: {"defaults": {...}, "overrides": {...}}

# List namespaces (empty after fresh install)
curl http://localhost:8181/v1/namespaces
```

## Spark configuration

Add these properties to any SparkSession that uses Iceberg:

```python
spark = SparkSession.builder \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.lakehouse.type", "rest") \
    .config("spark.sql.catalog.lakehouse.uri", "http://iceberg-rest-catalog.infrastructure.svc.cluster.local:8181") \
    .config("spark.sql.catalog.lakehouse.warehouse", "s3://lakehouse-warehouse/") \
    .config("spark.sql.catalog.lakehouse.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .config("spark.sql.catalog.lakehouse.s3.endpoint", "http://minio.infrastructure.svc.cluster.local:9000") \
    .config("spark.sql.catalog.lakehouse.s3.path-style-access", "true") \
    .getOrCreate()
```

## Trino configuration

```properties
# catalog/lakehouse.properties (mounted into Trino pod)
connector.name=iceberg
iceberg.catalog.type=rest
iceberg.rest-catalog.uri=http://iceberg-rest-catalog.infrastructure.svc.cluster.local:8181
iceberg.rest-catalog.warehouse=lakehouse
fs.native-s3.enabled=true
s3.endpoint=http://minio.infrastructure.svc.cluster.local:9000
s3.path-style-access=true
s3.aws-access-key=minioadmin
s3.aws-secret-key=minioadmin
s3.region=us-east-1
```

## Common issues

**Catalog fails to start — JDBC connection refused:**  
PostgreSQL is not yet ready. Check: `kubectl get pods -n infrastructure`  
The catalog's readiness probe calls `/v1/config` — it will stay NotReady until JDBC connects.

**Tables created by Spark not visible in Trino:**  
Verify both engines point to the same catalog URI and warehouse path. A mismatch in the warehouse path (`s3://` vs `s3a://`) will cause them to see different table metadata.
