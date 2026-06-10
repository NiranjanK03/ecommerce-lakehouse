"""
Bronze CDC ingestion — Spark Structured Streaming.

Reads Debezium CDC events from Kafka and writes them to Bronze Iceberg tables.

Design principles:
  - Append-only: Bronze tables are never updated or deleted. Every CDC event
    becomes exactly one new row.
  - Schema-agnostic: stores raw Debezium JSON (before_json / after_json).
    Silver handles column extraction and typing.
  - Exactly-once: Spark checkpoints committed Kafka offsets to MinIO after
    each successful micro-batch write.
  - One table per source: writes to lakehouse.bronze.<table_name> so Bronze
    tables mirror the source schema boundary.

OTel:
  - Emits pipeline.bronze.e2e_latency_seconds histogram using the PostgreSQL
    WAL commit timestamp (_cdc_source_ts_ms) embedded in each Debezium event.
    This is the only metric that can capture true source-to-Bronze latency —
    infrastructure metrics cannot see inside the Debezium payload.
"""

import os
import time
import logging
from typing import Iterator

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, IntegerType,
)

from opentelemetry import metrics
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.resources import Resource
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("bronze_ingest")

# ─── OTel setup ───────────────────────────────────────────────────────────────

OTLP_ENDPOINT = os.getenv(
    "OTEL_EXPORTER_OTLP_ENDPOINT",
    "http://otel-collector.observability.svc.cluster.local:4317",
)

reader = PeriodicExportingMetricReader(
    OTLPMetricExporter(endpoint=OTLP_ENDPOINT),
    export_interval_millis=15_000,
)
provider = MeterProvider(
    resource=Resource.create({"service.name": "spark-bronze-ingest"}),
    metric_readers=[reader],
)
metrics.set_meter_provider(provider)
meter = metrics.get_meter("bronze-ingestion")

# Histogram: seconds from PostgreSQL commit to Bronze Iceberg write.
# Label 'table' has 7 possible values — safe cardinality.
e2e_latency_hist = meter.create_histogram(
    name="pipeline.bronze.e2e_latency_seconds",
    description="Seconds from PostgreSQL WAL commit to Bronze Iceberg write",
    unit="s",
)
records_counter = meter.create_counter(
    name="pipeline.bronze.records_ingested_total",
    description="Total CDC events written to Bronze",
)
batch_duration_hist = meter.create_histogram(
    name="pipeline.bronze.batch_duration_seconds",
    description="Duration of each Spark micro-batch",
    unit="s",
)

# ─── Config ───────────────────────────────────────────────────────────────────

KAFKA_BOOTSTRAP = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "kafka.streaming.svc.cluster.local:9092",
)
ICEBERG_URI = os.getenv(
    "ICEBERG_REST_URI",
    "http://iceberg-rest-catalog.infrastructure.svc.cluster.local:8181",
)
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://minio.infrastructure.svc.cluster.local:9000")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "minioadmin")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "minioadmin")
CHECKPOINT_LOCATION = os.getenv(
    "CHECKPOINT_LOCATION",
    "s3a://lakehouse-checkpoints/bronze/",
)
TRIGGER_INTERVAL = os.getenv("TRIGGER_INTERVAL", "30 seconds")

TOPICS = [
    "olist.public.orders",
    "olist.public.order_items",
    "olist.public.customers",
    "olist.public.products",
    "olist.public.sellers",
    "olist.public.order_payments",
    "olist.public.order_reviews",
]

# ─── Spark session ─────────────────────────────────────────────────────────────

spark = (
    SparkSession.builder.appName("bronze-cdc-ingestion")
    # Iceberg Spark catalog
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lakehouse.type", "rest")
    .config("spark.sql.catalog.lakehouse.uri", ICEBERG_URI)
    .config("spark.sql.catalog.lakehouse.warehouse", "s3://lakehouse-warehouse/")
    .config("spark.sql.catalog.lakehouse.io-impl", "org.apache.iceberg.hadoop.HadoopFileIO")
    .config("spark.sql.catalog.lakehouse.s3.endpoint", S3_ENDPOINT)
    .config("spark.sql.catalog.lakehouse.s3.path-style-access", "true")
    # S3A filesystem for checkpoints and s3a:// paths
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
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ─── Debezium JSON schema ──────────────────────────────────────────────────────
# We parse only the envelope fields we need. The `before` and `after` payloads
# are kept as raw JSON strings — Bronze does not interpret source column values.

DEBEZIUM_SCHEMA = StructType([
    StructField("op",     StringType(), nullable=True),
    StructField("ts_ms",  LongType(),   nullable=True),   # Debezium processing time
    StructField("source", StructType([
        StructField("ts_ms", LongType(),  nullable=True),  # PostgreSQL commit time
        StructField("table", StringType(), nullable=True),
        StructField("db",    StringType(), nullable=True),
        StructField("lsn",   LongType(),  nullable=True),
    ]), nullable=True),
    # before / after are stored as raw JSON strings, not parsed structs.
    # Using StringType here; the outer from_json gives us the envelope,
    # then we extract before/after as sub-strings via get_json_object.
])

# ─── Batch processor ──────────────────────────────────────────────────────────

def process_batch(batch_df: DataFrame, batch_id: int) -> None:
    """
    Called by Spark for each micro-batch.

    Responsibilities:
      1. Parse the Debezium JSON envelope
      2. Route rows to the correct Bronze Iceberg table by source table name
      3. Emit OTel metrics (E2E latency, record count, batch duration)
    """
    if batch_df.isEmpty():
        log.info(f"Batch {batch_id}: empty, skipping")
        return

    batch_start = time.perf_counter()
    now_ms = int(time.time() * 1000)

    # Parse Debezium envelope from the Kafka `value` bytes
    parsed = (
        batch_df
        .select(
            F.col("topic"),
            F.col("partition").cast(IntegerType()).alias("_cdc_partition"),
            F.col("offset").cast("long").alias("_cdc_offset"),
            F.from_json(F.col("value").cast("string"), DEBEZIUM_SCHEMA).alias("envelope"),
            F.col("value").cast("string").alias("_raw"),
        )
        .select(
            F.col("topic").alias("_cdc_topic"),
            F.col("_cdc_partition"),
            F.col("_cdc_offset"),
            F.col("envelope.op").alias("_cdc_op"),
            F.col("envelope.ts_ms").alias("_cdc_event_ts_ms"),
            F.col("envelope.source.ts_ms").alias("_cdc_source_ts_ms"),
            F.col("envelope.source.table").alias("_source_table"),
            F.current_timestamp().alias("_cdc_ingested_at"),
            # Store before/after as raw JSON strings extracted from the envelope
            F.get_json_object(F.col("_raw"), "$.before").alias("before_json"),
            F.get_json_object(F.col("_raw"), "$.after").alias("after_json"),
        )
        # Drop rows where envelope parsing failed (malformed JSON)
        .filter(F.col("_cdc_op").isNotNull())
    )

    # Collect OTel metrics from this batch
    rows = parsed.select(
        "_cdc_source_ts_ms", "_source_table"
    ).collect()

    for row in rows:
        source_ts = row["_cdc_source_ts_ms"] or now_ms
        table = row["_source_table"] or "unknown"
        latency_s = max(0.0, (now_ms - source_ts) / 1000.0)
        e2e_latency_hist.record(latency_s, {"table": table})

    records_counter.add(len(rows))

    # Route to per-table Bronze Iceberg tables
    tables_in_batch = [r["_source_table"] for r in rows if r["_source_table"]]
    for table_name in set(tables_in_batch):
        table_df = (
            parsed
            .filter(F.col("_source_table") == table_name)
            .drop("_source_table")
        )
        target = f"lakehouse.bronze.{table_name}"
        table_df.writeTo(target).append()
        log.info(f"Batch {batch_id}: wrote {table_df.count()} rows → {target}")

    batch_duration = time.perf_counter() - batch_start
    batch_duration_hist.record(batch_duration)
    log.info(f"Batch {batch_id}: {len(rows)} total rows in {batch_duration:.2f}s")


# ─── Streaming query ───────────────────────────────────────────────────────────

raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", ",".join(TOPICS))
    .option("startingOffsets", "earliest")
    .option("kafka.group.id", "bronze-consumer-group")
    # Cap initial catchup: process at most 50k records per topic per trigger.
    # Without this, the first batch tries to consume all 500k+ backlog at once,
    # which OOM-kills the driver before any data lands in Iceberg.
    .option("maxOffsetsPerTrigger", 50_000)
    # Don't fail if a Kafka partition is temporarily unavailable (k3d restart)
    .option("failOnDataLoss", "false")
    .load()
)

query = (
    raw_stream.writeStream
    .foreachBatch(process_batch)
    .option("checkpointLocation", CHECKPOINT_LOCATION)
    # Micro-batch every 30 seconds. Balances latency vs Iceberg small-file overhead.
    # Lower = more latency but more small files in MinIO (compaction needed sooner).
    .trigger(processingTime=TRIGGER_INTERVAL)
    .start()
)

log.info(f"Bronze ingestion streaming started. Topics: {TOPICS}")
query.awaitTermination()
