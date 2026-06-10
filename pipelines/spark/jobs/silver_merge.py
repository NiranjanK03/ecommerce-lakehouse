"""
Silver MERGE INTO — PySpark batch job.

Reads new Bronze CDC events (incremental, since last Silver run) and
merges them into Silver Iceberg tables using MERGE INTO semantics.

Key design decisions:

  Incremental read from Bronze:
    Reads only Bronze rows where _cdc_ingested_at > watermark. The watermark
    is passed by Airflow as an environment variable (SILVER_WATERMARK_TS)
    derived from the DAG's data_interval_start. On the very first run,
    watermark is 1970-01-01 (read all Bronze).

  Deduplication before merge:
    Bronze is append-only — the same primary key can appear multiple times
    if a row was updated several times in a single Bronze batch. We keep
    only the event with the latest _cdc_source_ts_ms per primary key before
    merging into Silver.

  Soft deletes:
    When op = 'd', we set _deleted = true in Silver rather than physically
    removing the row. This preserves the row for downstream joins in Gold
    and for audit queries. Gold filters out _deleted = true rows.

  Idempotency:
    MERGE INTO on primary key is inherently idempotent — re-running with the
    same Bronze rows produces the same Silver state. Airflow can safely retry
    this job on failure without manual cleanup.

  Per-table processing:
    Each source table is processed independently. If orders MERGE succeeds
    but customers fails, Airflow retries the full job — orders MERGE is a
    no-op (same rows, already in Silver) while customers MERGE re-runs.
"""

from __future__ import annotations

import os
import logging
from datetime import datetime
from typing import NamedTuple

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DecimalType, IntegerType, TimestampNTZType, DateType, StringType, BooleanType,
)

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("silver_merge")

# ─── Config ───────────────────────────────────────────────────────────────────

ICEBERG_URI    = os.getenv("ICEBERG_REST_URI",   "http://iceberg-rest-catalog.infrastructure.svc.cluster.local:8181")
S3_ENDPOINT    = os.getenv("S3_ENDPOINT",         "http://minio.infrastructure.svc.cluster.local:9000")
S3_ACCESS_KEY  = os.getenv("S3_ACCESS_KEY",       "minioadmin")
S3_SECRET_KEY  = os.getenv("S3_SECRET_KEY",       "minioadmin")

# Watermark: ISO timestamp passed by Airflow (data_interval_start).
# Bronze rows ingested after this timestamp are considered new.
WATERMARK_TS = os.getenv("SILVER_WATERMARK_TS", "1970-01-01T00:00:00")

# ─── Table configuration ──────────────────────────────────────────────────────

class TableConfig(NamedTuple):
    pk_cols: list[str]           # primary key column(s) for MERGE ON clause
    column_map: dict[str, tuple] # Silver column → (JSON path, Spark cast expression)


def _j(path: str, cast=None) -> tuple:
    """Helper: JSON path + optional cast."""
    return (path, cast)


TABLE_CONFIGS: dict[str, TableConfig] = {
    "orders": TableConfig(
        pk_cols=["order_id"],
        column_map={
            "order_id":                ("$.order_id",                    StringType()),
            "customer_id":             ("$.customer_id",                 StringType()),
            "status":                  ("$.order_status",                StringType()),
            "purchased_at":            ("$.order_purchase_timestamp",    TimestampNTZType()),
            "approved_at":             ("$.order_approved_at",           TimestampNTZType()),
            "delivered_carrier_at":    ("$.order_delivered_carrier",     TimestampNTZType()),
            "delivered_customer_at":   ("$.order_delivered_customer",    TimestampNTZType()),
            "estimated_delivery_date": ("$.order_estimated_delivery",    DateType()),
        },
    ),
    "order_items": TableConfig(
        pk_cols=["order_id", "order_item_id"],
        column_map={
            "order_id":            ("$.order_id",            StringType()),
            "order_item_id":       ("$.order_item_id",       IntegerType()),
            "product_id":          ("$.product_id",          StringType()),
            "seller_id":           ("$.seller_id",           StringType()),
            "shipping_limit_date": ("$.shipping_limit_date", TimestampNTZType()),
            "price":               ("$.price",               DecimalType(10, 2)),
            "freight_value":       ("$.freight_value",       DecimalType(10, 2)),
        },
    ),
    "customers": TableConfig(
        pk_cols=["customer_id"],
        column_map={
            "customer_id":        ("$.customer_id",        StringType()),
            "customer_unique_id": ("$.customer_unique_id", StringType()),
            "customer_zip_code":  ("$.customer_zip_code",  StringType()),
            "customer_city":      ("$.customer_city",      StringType()),
            "customer_state":     ("$.customer_state",     StringType()),
        },
    ),
    "products": TableConfig(
        pk_cols=["product_id"],
        column_map={
            "product_id":           ("$.product_id",           StringType()),
            "product_category":     ("$.product_category",     StringType()),
            "product_name_length":  ("$.product_name_length",  IntegerType()),
            "product_desc_length":  ("$.product_desc_length",  IntegerType()),
            "product_photos_qty":   ("$.product_photos_qty",   IntegerType()),
            "product_weight_g":     ("$.product_weight_g",     IntegerType()),
            "product_length_cm":    ("$.product_length_cm",    IntegerType()),
            "product_height_cm":    ("$.product_height_cm",    IntegerType()),
            "product_width_cm":     ("$.product_width_cm",     IntegerType()),
        },
    ),
    "sellers": TableConfig(
        pk_cols=["seller_id"],
        column_map={
            "seller_id":       ("$.seller_id",       StringType()),
            "seller_zip_code": ("$.seller_zip_code", StringType()),
            "seller_city":     ("$.seller_city",     StringType()),
            "seller_state":    ("$.seller_state",    StringType()),
        },
    ),
    "order_payments": TableConfig(
        pk_cols=["order_id", "payment_sequential"],
        column_map={
            "order_id":             ("$.order_id",             StringType()),
            "payment_sequential":   ("$.payment_sequential",   IntegerType()),
            "payment_type":         ("$.payment_type",         StringType()),
            "payment_installments": ("$.payment_installments", IntegerType()),
            "payment_value":        ("$.payment_value",        DecimalType(10, 2)),
        },
    ),
    "order_reviews": TableConfig(
        pk_cols=["review_id", "order_id"],
        column_map={
            "review_id":            ("$.review_id",            StringType()),
            "order_id":             ("$.order_id",             StringType()),
            "review_score":         ("$.review_score",         IntegerType()),
            "review_comment_title": ("$.review_comment_title", StringType()),
            "review_comment_msg":   ("$.review_comment_msg",   StringType()),
            "review_creation_date": ("$.review_creation_date", TimestampNTZType()),
            "review_answer_date":   ("$.review_answer_date",   TimestampNTZType()),
        },
    ),
}

# ─── Spark session ─────────────────────────────────────────────────────────────

spark = (
    SparkSession.builder.appName("silver-merge")
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
    # Route s3:// URIs through S3A — Iceberg REST catalog stores table locations as
    # s3:// (from the warehouse prefix), but Hadoop only knows s3a://.
    .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3.endpoint", S3_ENDPOINT)
    .config("spark.hadoop.fs.s3.access.key", S3_ACCESS_KEY)
    .config("spark.hadoop.fs.s3.secret.key", S3_SECRET_KEY)
    .config("spark.hadoop.fs.s3.path.style.access", "true")
    # Write timestamps as Iceberg `timestamp` (no timezone) so StarRocks iceberg_catalog
    # can read them. Without this Spark writes TIMESTAMPTZ which StarRocks maps to NULL.
    .config("spark.sql.iceberg.handle-timestamp-without-timezone", "true")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ─── Core merge logic ─────────────────────────────────────────────────────────

def read_new_bronze(table_name: str, watermark_ts: str) -> DataFrame:
    """Read Bronze rows ingested after the watermark timestamp."""
    return (
        spark.read
        .format("iceberg")
        .load(f"lakehouse.bronze.{table_name}")
        .filter(F.col("_cdc_ingested_at") > F.lit(watermark_ts).cast("timestamp"))
    )


def parse_to_silver(bronze_df: DataFrame, config: TableConfig) -> DataFrame:
    """
    Parse raw Bronze JSON into typed Silver columns.

    Steps:
      1. Extract typed columns from after_json using get_json_object
      2. For deletes (op='d'), source is before_json (after is null)
      3. Deduplicate: keep latest event per primary key
      4. Add Silver metadata columns
    """
    # Use after_json for inserts/updates, before_json for deletes
    payload = F.when(
        F.col("_cdc_op") == "d",
        F.col("before_json"),
    ).otherwise(F.col("after_json"))

    # Extract and cast each Silver column from the JSON payload.
    # Debezium encodes PostgreSQL TIMESTAMP as epoch microseconds (long string)
    # and DATE as epoch days (int string). Direct .cast() would produce NULL
    # for these — apply the correct numeric conversion first.
    col_exprs = []
    for silver_col, (json_path, spark_type) in config.column_map.items():
        raw = F.get_json_object(payload, json_path)
        if isinstance(spark_type, TimestampNTZType):
            # Debezium encodes timestamps as epoch microseconds.
            # Spark cannot cast DOUBLE → TIMESTAMP_NTZ directly; go via
            # timestamp_seconds() (creates TIMESTAMP with tz) then strip tz.
            expr = F.timestamp_seconds(raw.cast("long") / F.lit(1_000_000)).cast("timestamp_ntz")
        elif isinstance(spark_type, DateType):
            expr = F.to_date(F.from_unixtime(raw.cast("long") * F.lit(86400)))
        else:
            expr = raw.cast(spark_type)
        col_exprs.append(expr.alias(silver_col))

    # CDC metadata passthrough
    col_exprs += [
        F.col("_cdc_op"),
        F.col("_cdc_source_ts_ms"),
        F.col("_cdc_offset"),
        (F.col("_cdc_op") == "d").alias("is_deleted"),
        F.current_timestamp().cast("timestamp_ntz").alias("_silver_updated_at"),
    ]

    parsed = bronze_df.select(*col_exprs)

    # Deduplicate: for the same PK, keep the row with the latest source timestamp.
    # This handles multiple CDC events for the same row in one Bronze batch.
    window = (
        __import__("pyspark.sql.window", fromlist=["Window"])
        .Window.partitionBy(*config.pk_cols)
        .orderBy(F.col("_cdc_source_ts_ms").desc())
    )
    return (
        parsed
        .withColumn("_row_rank", F.row_number().over(window))
        .filter(F.col("_row_rank") == 1)
        .drop("_row_rank")
    )


def build_merge_sql(table_name: str, config: TableConfig) -> str:
    """
    Build the MERGE INTO SQL for this table.

    Matched + not deleted → UPDATE all columns
    Matched + deleted     → UPDATE _deleted = true
    Not matched           → INSERT new row
    """
    silver_table = f"lakehouse.silver.{table_name}"
    pk_condition = " AND ".join(
        f"t.{c} = s.{c}" for c in config.pk_cols
    )
    silver_cols = list(config.column_map.keys()) + [
        "_cdc_op", "_cdc_source_ts_ms", "_cdc_offset", "is_deleted", "_silver_updated_at"
    ]
    update_set = ", ".join(f"t.{c} = s.{c}" for c in silver_cols)
    insert_cols = ", ".join(silver_cols)
    insert_vals = ", ".join(f"s.{c}" for c in silver_cols)

    return f"""
        MERGE INTO {silver_table} t
        USING silver_staging_{table_name} s
        ON {pk_condition}
        WHEN MATCHED AND s.is_deleted = true
          THEN UPDATE SET t.is_deleted = true, t._cdc_op = s._cdc_op,
                          t._cdc_source_ts_ms = s._cdc_source_ts_ms,
                          t._silver_updated_at = s._silver_updated_at
        WHEN MATCHED
          THEN UPDATE SET {update_set}
        WHEN NOT MATCHED
          THEN INSERT ({insert_cols}) VALUES ({insert_vals})
    """


def merge_table(table_name: str, watermark_ts: str) -> int:
    """
    Full incremental merge for one source table.
    Returns the number of Bronze rows processed.
    """
    config = TABLE_CONFIGS[table_name]

    bronze_new = read_new_bronze(table_name, watermark_ts)
    count = bronze_new.count()

    if count == 0:
        log.info("%s: no new Bronze rows since %s — skipping", table_name, watermark_ts)
        return 0

    log.info("%s: processing %d new Bronze rows", table_name, count)

    silver_staging = parse_to_silver(bronze_new, config)

    # Register as a temp view so Spark SQL MERGE INTO can reference it
    view_name = f"silver_staging_{table_name}"
    silver_staging.createOrReplaceTempView(view_name)

    merge_sql = build_merge_sql(table_name, config)
    spark.sql(merge_sql)

    log.info("%s: MERGE INTO completed — %d rows merged", table_name, count)
    return count


# ─── Main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    log.info("Silver merge starting. Watermark: %s", WATERMARK_TS)
    total = 0
    for table in TABLE_CONFIGS:
        rows = merge_table(table, WATERMARK_TS)
        total += rows

    log.info("Silver merge complete. Total rows processed: %d", total)


if __name__ == "__main__":
    main()
    spark.stop()
