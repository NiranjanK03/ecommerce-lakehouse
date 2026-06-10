# ADR-001: Config-Driven Storage Portability

**Status:** Accepted  
**Date:** 2026-05-14

## Context

The lakehouse needs object storage for Iceberg data files, Spark checkpoints, and StarRocks shared-data volumes. Three realistic targets exist: MinIO (self-hosted S3-compatible), AWS S3, and Azure ADLS Gen2. The project must run locally without cloud costs during development while remaining deployable to AWS or Azure with minimal effort.

## Decision

All storage access goes through Iceberg's `S3FileIO` driver and Spark's `S3AFileSystem` — both of which speak the S3 REST API. MinIO implements that API completely, so no application code changes are needed to switch providers. The only thing that changes between environments is the endpoint URL, bucket name, and credentials, which live in `config/local.yaml` (gitignored).

In practice:
- Spark reads/writes Iceberg data via `spark.hadoop.fs.s3a.*` config pointing at MinIO
- StarRocks reads Silver Iceberg files via its external catalog, also via S3 config
- StarRocks stores Gold MV data in a separate MinIO bucket via its shared-data storage volume

To migrate to AWS S3: replace the MinIO endpoint with the S3 endpoint and swap access keys. To migrate to ADLS: use the `abfs://` scheme with Azure credentials. No pipeline code changes.

## Consequences

**Good:** Zero application code changes to switch providers. Local dev has no cloud account dependency. Iceberg's `S3FileIO` is the same driver used in production AWS deployments.

**Bad:** `config/local.yaml` contains plaintext credentials — in a real deployment these would come from k8s Secrets or a secrets manager injected as environment variables into the SparkApplication CRDs and StarRocks operator.

## Alternatives Considered

**Custom storage adapter classes:** An earlier design used `MinIOAdapter`, `S3Adapter`, `ADLSAdapter` Python classes wrapping boto3/azure-sdk. Rejected — the adapters duplicated what Iceberg's `S3FileIO` and Spark's `S3AFileSystem` already provide natively. Two abstraction layers for the same thing adds complexity with no benefit.

**Hardcode MinIO:** Simpler but defeats the portability argument. Every production deployment would require code changes.
