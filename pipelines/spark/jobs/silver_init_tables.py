"""
Silver Iceberg table initialisation.

Creates typed Silver tables for all 7 Olist source tables.
Run once before the first Silver merge job executes.

Silver schema design principles:
  - Typed columns: Bronze stores raw JSON strings; Silver has proper Spark types
  - Soft deletes: _deleted flag instead of physical row removal
  - CDC audit trail: _cdc_* columns preserved for debugging and audit
  - Partitioned for query efficiency: Silver queries from Gold filter by date

Run:
  make init-silver-tables
"""

import os
from pyspark.sql import SparkSession

ICEBERG_URI = os.getenv(
    "ICEBERG_REST_URI",
    "http://iceberg-rest-catalog.infrastructure.svc.cluster.local:8181",
)
S3_ENDPOINT  = os.getenv("S3_ENDPOINT",   "http://minio.infrastructure.svc.cluster.local:9000")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "minioadmin")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "minioadmin")

spark = (
    SparkSession.builder.appName("silver-init-tables")
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

spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.silver")

SILVER_TABLES = {
    "orders": """
        CREATE TABLE IF NOT EXISTS lakehouse.silver.orders (
            order_id                  STRING        NOT NULL,
            customer_id               STRING,
            status                    STRING,
            purchased_at              TIMESTAMP_NTZ,
            approved_at               TIMESTAMP_NTZ,
            delivered_carrier_at      TIMESTAMP_NTZ,
            delivered_customer_at     TIMESTAMP_NTZ,
            estimated_delivery_date   DATE,
            _cdc_op                   STRING,
            _cdc_source_ts_ms         BIGINT,
            _cdc_offset               BIGINT,
            is_deleted                BOOLEAN       NOT NULL,
            _silver_updated_at        TIMESTAMP_NTZ NOT NULL
        )
        USING iceberg
        PARTITIONED BY (months(purchased_at))
        TBLPROPERTIES (
            'write.format.default'     = 'parquet',
            'write.merge.mode'         = 'merge-on-read',
            'write.update.mode'        = 'merge-on-read',
            'write.delete.mode'        = 'merge-on-read'
        )
    """,

    "order_items": """
        CREATE TABLE IF NOT EXISTS lakehouse.silver.order_items (
            order_id              STRING   NOT NULL,
            order_item_id         INT      NOT NULL,
            product_id            STRING,
            seller_id             STRING,
            shipping_limit_date   TIMESTAMP_NTZ,
            price                 DECIMAL(10,2),
            freight_value         DECIMAL(10,2),
            _cdc_op               STRING,
            _cdc_source_ts_ms     BIGINT,
            _cdc_offset           BIGINT,
            is_deleted            BOOLEAN  NOT NULL,
            _silver_updated_at    TIMESTAMP_NTZ NOT NULL
        )
        USING iceberg
        PARTITIONED BY (bucket(8, order_id))
        TBLPROPERTIES ('write.format.default' = 'parquet')
    """,

    "customers": """
        CREATE TABLE IF NOT EXISTS lakehouse.silver.customers (
            customer_id           STRING   NOT NULL,
            customer_unique_id    STRING,
            customer_zip_code     STRING,
            customer_city         STRING,
            customer_state        STRING,
            _cdc_op               STRING,
            _cdc_source_ts_ms     BIGINT,
            _cdc_offset           BIGINT,
            is_deleted            BOOLEAN  NOT NULL,
            _silver_updated_at    TIMESTAMP_NTZ NOT NULL
        )
        USING iceberg
        PARTITIONED BY (customer_state)
        TBLPROPERTIES ('write.format.default' = 'parquet')
    """,

    "products": """
        CREATE TABLE IF NOT EXISTS lakehouse.silver.products (
            product_id            STRING   NOT NULL,
            product_category      STRING,
            product_name_length   INT,
            product_desc_length   INT,
            product_photos_qty    INT,
            product_weight_g      INT,
            product_length_cm     INT,
            product_height_cm     INT,
            product_width_cm      INT,
            _cdc_op               STRING,
            _cdc_source_ts_ms     BIGINT,
            _cdc_offset           BIGINT,
            is_deleted            BOOLEAN  NOT NULL,
            _silver_updated_at    TIMESTAMP_NTZ NOT NULL
        )
        USING iceberg
        PARTITIONED BY (product_category)
        TBLPROPERTIES ('write.format.default' = 'parquet')
    """,

    "sellers": """
        CREATE TABLE IF NOT EXISTS lakehouse.silver.sellers (
            seller_id             STRING   NOT NULL,
            seller_zip_code       STRING,
            seller_city           STRING,
            seller_state          STRING,
            _cdc_op               STRING,
            _cdc_source_ts_ms     BIGINT,
            _cdc_offset           BIGINT,
            is_deleted            BOOLEAN  NOT NULL,
            _silver_updated_at    TIMESTAMP_NTZ NOT NULL
        )
        USING iceberg
        PARTITIONED BY (seller_state)
        TBLPROPERTIES ('write.format.default' = 'parquet')
    """,

    "order_payments": """
        CREATE TABLE IF NOT EXISTS lakehouse.silver.order_payments (
            order_id              STRING   NOT NULL,
            payment_sequential    INT      NOT NULL,
            payment_type          STRING,
            payment_installments  INT,
            payment_value         DECIMAL(10,2),
            _cdc_op               STRING,
            _cdc_source_ts_ms     BIGINT,
            _cdc_offset           BIGINT,
            is_deleted            BOOLEAN  NOT NULL,
            _silver_updated_at    TIMESTAMP_NTZ NOT NULL
        )
        USING iceberg
        PARTITIONED BY (bucket(8, order_id))
        TBLPROPERTIES ('write.format.default' = 'parquet')
    """,

    "order_reviews": """
        CREATE TABLE IF NOT EXISTS lakehouse.silver.order_reviews (
            review_id             STRING   NOT NULL,
            order_id              STRING   NOT NULL,
            review_score          INT,
            review_comment_title  STRING,
            review_comment_msg    STRING,
            review_creation_date  TIMESTAMP_NTZ,
            review_answer_date    TIMESTAMP_NTZ,
            _cdc_op               STRING,
            _cdc_source_ts_ms     BIGINT,
            _cdc_offset           BIGINT,
            is_deleted            BOOLEAN  NOT NULL,
            _silver_updated_at    TIMESTAMP_NTZ NOT NULL
        )
        USING iceberg
        PARTITIONED BY (months(review_creation_date))
        TBLPROPERTIES ('write.format.default' = 'parquet')
    """,
}

for table, ddl in SILVER_TABLES.items():
    # DROP first so schema changes take effect on re-runs (safe in dev — Silver is rebuilt from Bronze)
    spark.sql(f"DROP TABLE IF EXISTS lakehouse.silver.{table}")
    print(f"Creating lakehouse.silver.{table} ...")
    spark.sql(ddl)
    print(f"  OK")

print("\nSilver namespace tables:")
spark.sql("SHOW TABLES IN lakehouse.silver").show()

spark.stop()
