"""
Bronze initial bulk load — reads all 7 tables from PostgreSQL via JDBC
and writes them to Bronze Iceberg tables in the same format as the
Debezium streaming job.

Use this instead of Debezium's snapshot mode when the dataset is large
(>500k rows). Debezium snapshot keeps a consistent snapshot transaction
open for the full duration, which causes OOM for multi-million-row datasets.

This job emulates Debezium snapshot events:
  - op = "r"  (read = snapshot)
  - before_json = null
  - after_json = full row as JSON

After this job completes:
  - Register the Debezium connector with snapshot.mode=no_data
  - The connector will stream WAL changes only (no re-snapshot)
  - Silver MERGE handles deduplication between bulk load and WAL events

Run via SparkApplication: pipelines/spark/applications/bronze-initial-load.yaml
"""

import os
import logging
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("bronze_initial_load")

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres-postgresql.infrastructure.svc.cluster.local")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB   = os.getenv("POSTGRES_DB", "olist")
POSTGRES_USER = os.getenv("POSTGRES_USER", "olist_user")
POSTGRES_PASS = os.getenv("POSTGRES_PASS", "olist_pass")

JDBC_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

TABLES = [
    "orders",
    "order_items",
    "customers",
    "products",
    "sellers",
    "order_payments",
    "order_reviews",
]

# Number of JDBC partitions per table — controls parallelism
# Higher = more executors reading in parallel but more DB connections
JDBC_PARTITIONS = 4


def jdbc_opts(table: str, num_partitions: int) -> dict:
    """JDBC read options with partitioning for parallel reads."""
    return {
        "url": JDBC_URL,
        "dbtable": f"public.{table}",
        "user": POSTGRES_USER,
        "password": POSTGRES_PASS,
        "driver": "org.postgresql.Driver",
        "fetchsize": "10000",
        "numPartitions": str(num_partitions),
        # Partition by ctid (physical row position) for even splits without needing a numeric PK
        "partitionColumn": "ctid::text::point[0]::int8",
        "lowerBound": "0",
        "upperBound": "1000000000",
    }


def load_table(spark: SparkSession, table: str):
    ingest_ts = datetime.now(timezone.utc)
    epoch_ms = int(ingest_ts.timestamp() * 1000)

    log.info(f"Reading {table} from PostgreSQL via JDBC...")

    # Read the table
    df = (
        spark.read
        .format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", f"public.{table}")
        .option("user", POSTGRES_USER)
        .option("password", POSTGRES_PASS)
        .option("driver", "org.postgresql.Driver")
        .option("fetchsize", "50000")
        .load()
    )

    row_count = df.count()
    log.info(f"{table}: {row_count:,} rows read from PostgreSQL")

    # Convert each row to JSON (the after_json field in Bronze)
    df_json = df.select(
        F.to_json(F.struct([F.col(c) for c in df.columns])).alias("after_json")
    )

    # Add Bronze CDC envelope columns — mimics Debezium snapshot event format
    df_bronze = df_json.select(
        F.lit("r").alias("_cdc_op"),                          # op=r: snapshot read
        F.lit(epoch_ms).cast("long").alias("_cdc_source_ts_ms"),
        F.lit(epoch_ms).cast("long").alias("_cdc_event_ts_ms"),
        F.lit(ingest_ts).cast("timestamp").alias("_cdc_ingested_at"),
        F.lit(f"olist.public.{table}").alias("_cdc_topic"),
        F.lit(0).alias("_cdc_partition"),
        F.lit(0).cast("long").alias("_cdc_offset"),
        F.lit(None).cast("string").alias("before_json"),
        F.col("after_json"),
    )

    target = f"lakehouse.bronze.{table}"
    log.info(f"Writing {row_count:,} rows → {target}")
    df_bronze.writeTo(target).append()
    log.info(f"{table}: done ({row_count:,} rows written to Bronze)")
    return row_count


def main():
    spark = (
        SparkSession.builder
        .appName("bronze-initial-load")
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.lakehouse",
                "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.type", "rest")
        .config("spark.sql.catalog.lakehouse.uri",
                "http://iceberg-rest-catalog.infrastructure.svc.cluster.local:8181")
        .config("spark.sql.catalog.lakehouse.warehouse", "s3://lakehouse-warehouse/")
        .config("spark.sql.catalog.lakehouse.io-impl",
                "org.apache.iceberg.hadoop.HadoopFileIO")
        .config("spark.sql.catalog.lakehouse.s3.endpoint",
                "http://minio.infrastructure.svc.cluster.local:9000")
        .config("spark.sql.catalog.lakehouse.s3.path-style-access", "true")
        .config("spark.hadoop.fs.s3a.endpoint",
                "http://minio.infrastructure.svc.cluster.local:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3.endpoint",
                "http://minio.infrastructure.svc.cluster.local:9000")
        .config("spark.hadoop.fs.s3.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3.secret.key", "minioadmin")
        .config("spark.hadoop.fs.s3.path.style.access", "true")
        .config("spark.sql.iceberg.handle-timestamp-without-timezone", "true")
        .getOrCreate()
    )

    total = 0
    for table in TABLES:
        try:
            n = load_table(spark, table)
            total += n
        except Exception as e:
            log.error(f"Failed to load {table}: {e}")
            raise

    log.info(f"Bronze initial load complete. {total:,} total rows written.")
    spark.stop()


if __name__ == "__main__":
    main()
