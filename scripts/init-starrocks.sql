-- StarRocks initialisation — run once after FE pod is healthy
-- Requires: port-forward on localhost:9030 (run 'make port-forward' first)
-- Run via: mysql -h 127.0.0.1 -P 9030 -u root --password='' < scripts/init-starrocks.sql

-- ── 1. Storage volume pointing at MinIO ──────────────────────────────────────
-- enable_path_style_access = true is REQUIRED for MinIO.
-- Without it StarRocks constructs virtual-hosted-style URLs (bucket.host/key)
-- which MinIO doesn't support — requests fail with 403.
CREATE STORAGE VOLUME IF NOT EXISTS minio_volume
TYPE = S3
LOCATIONS = ('s3://lakehouse-starrocks/starrocks/')
PROPERTIES (
    "enabled"                         = "true",
    "aws.s3.region"                   = "us-east-1",
    "aws.s3.endpoint"                 = "http://minio.infrastructure.svc.cluster.local:9000",
    "aws.s3.access_key"               = "minioadmin",
    "aws.s3.secret_key"               = "minioadmin",
    "aws.s3.enable_path_style_access" = "true"
);

-- ── 2. Make it the default ────────────────────────────────────────────────────
-- All new StarRocks tables (including Gold MVs) use this volume unless overridden.
SET minio_volume AS DEFAULT STORAGE VOLUME;

-- ── 3. External Iceberg catalog ───────────────────────────────────────────────
-- Registers the Iceberg REST Catalog as an external catalog named iceberg_catalog.
-- No data is copied. StarRocks reads Silver Parquet files directly from MinIO.
-- Query path:
--   FE asks REST catalog: "which Parquet files are in silver.orders?"
--   REST catalog returns the file list from lakehouse-warehouse/
--   CN pods stream-read those Parquet files (vectorised scan)
--   FE aggregates and returns result
CREATE EXTERNAL CATALOG IF NOT EXISTS iceberg_catalog
COMMENT 'Iceberg REST Catalog on MinIO — Silver namespace'
PROPERTIES (
    "type"                              = "iceberg",
    "iceberg.catalog.type"             = "rest",
    "iceberg.catalog.uri"              = "http://iceberg-rest-catalog.infrastructure.svc.cluster.local:8181",
    "iceberg.catalog.warehouse"        = "s3://lakehouse-warehouse/",
    "aws.s3.endpoint"                  = "http://minio.infrastructure.svc.cluster.local:9000",
    "aws.s3.region"                    = "us-east-1",
    "aws.s3.access_key"                = "minioadmin",
    "aws.s3.secret_key"                = "minioadmin",
    "aws.s3.enable_path_style_access"  = "true"
);

-- ── 4. Gold database ──────────────────────────────────────────────────────────
-- Stores the materialized view aggregate rows (much smaller than Silver row data).
-- MV data is physically written to lakehouse-starrocks/ in MinIO.
CREATE DATABASE IF NOT EXISTS gold;

-- ── Verify ────────────────────────────────────────────────────────────────────
SHOW STORAGE VOLUMES;
SHOW CATALOGS;
SHOW DATABASES;
