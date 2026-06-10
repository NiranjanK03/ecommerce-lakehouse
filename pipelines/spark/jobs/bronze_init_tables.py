"""
Bronze Iceberg table initialisation.

Creates all Bronze tables in the Iceberg REST catalog before the streaming
job runs. Tables are partitioned by ingestion day for efficient time-based
pruning in Silver queries.

Run once before deploying the Bronze streaming SparkApplication:
  spark-submit bronze_init_tables.py

Or via the Makefile:
  make init-bronze-tables
"""

import os
from pyspark.sql import SparkSession

ICEBERG_URI = os.getenv(
    "ICEBERG_REST_URI",
    "http://iceberg-rest-catalog.infrastructure.svc.cluster.local:8181",
)
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://minio.infrastructure.svc.cluster.local:9000")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "minioadmin")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "minioadmin")

spark = (
    SparkSession.builder.appName("bronze-init-tables")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lakehouse.type", "rest")
    .config("spark.sql.catalog.lakehouse.uri", ICEBERG_URI)
    .config("spark.sql.catalog.lakehouse.warehouse", "s3://lakehouse-warehouse/")
    .config("spark.sql.catalog.lakehouse.io-impl", "org.apache.iceberg.hadoop.HadoopFileIO")
    .config("spark.sql.catalog.lakehouse.s3.endpoint", S3_ENDPOINT)
    .config("spark.sql.catalog.lakehouse.s3.path-style-access", "true")
    .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET_KEY)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)

spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.bronze")

# All Bronze tables share the same CDC envelope schema.
# The schema is intentionally agnostic to source table columns —
# Bronze stores raw Debezium JSON payloads. Silver handles parsing and typing.
#
# Columns:
#   _cdc_op           : Debezium operation type (c=create, u=update, d=delete, r=snapshot read)
#   _cdc_source_ts_ms : PostgreSQL WAL commit timestamp — used for E2E latency metric
#   _cdc_event_ts_ms  : Debezium connector processing timestamp
#   _cdc_ingested_at  : Spark write timestamp — partition key
#   _cdc_topic        : Kafka topic (e.g. olist.public.orders)
#   _cdc_partition    : Kafka partition number
#   _cdc_offset       : Kafka offset — used for deduplication in Silver
#   before_json       : Raw JSON of the row state BEFORE the change (null for inserts)
#   after_json        : Raw JSON of the row state AFTER the change (null for deletes)

BRONZE_DDL = """
CREATE TABLE IF NOT EXISTS lakehouse.bronze.{table} (
    _cdc_op             STRING        NOT NULL COMMENT 'c=create u=update d=delete r=snapshot',
    _cdc_source_ts_ms   BIGINT        NOT NULL COMMENT 'PostgreSQL WAL commit time (ms since epoch)',
    _cdc_event_ts_ms    BIGINT        NOT NULL COMMENT 'Debezium connector processing time',
    _cdc_ingested_at    TIMESTAMP     NOT NULL COMMENT 'Spark write time — partition key',
    _cdc_topic          STRING        NOT NULL COMMENT 'Kafka topic name',
    _cdc_partition      INT           NOT NULL COMMENT 'Kafka partition number',
    _cdc_offset         BIGINT        NOT NULL COMMENT 'Kafka offset within partition',
    before_json         STRING                 COMMENT 'Raw JSON of before image',
    after_json          STRING                 COMMENT 'Raw JSON of after image'
)
USING iceberg
PARTITIONED BY (days(_cdc_ingested_at))
TBLPROPERTIES (
    'write.format.default'      = 'parquet',
    'write.parquet.compression-codec' = 'snappy',
    'write.metadata.delete-after-commit.enabled' = 'true',
    'write.metadata.previous-versions-max' = '10'
)
"""

TABLES = [
    "orders",
    "order_items",
    "customers",
    "products",
    "sellers",
    "order_payments",
    "order_reviews",
]

for table in TABLES:
    print(f"Creating lakehouse.bronze.{table} ...")
    spark.sql(BRONZE_DDL.format(table=table))
    print(f"  OK")

print("\nBronze namespace tables:")
spark.sql("SHOW TABLES IN lakehouse.bronze").show()

spark.stop()
