"""
Pipeline performance benchmark.

Measures three things that query-engine benchmarks cannot:

1. Bronze ingestion throughput
   Rows/sec from PostgreSQL WAL commit → Iceberg Parquet write on MinIO.
   Derived from Bronze table metadata: total rows / (last_ingested - first_ingested).

2. E2E commit-to-Bronze latency
   Time from PostgreSQL WAL commit (_cdc_source_ts_ms) to Iceberg write (_cdc_ingested_at).
   Reports P50 / P95 / P99 across all Bronze rows.
   This is the only metric that captures true source-to-storage lag — infrastructure
   metrics cannot see inside the Debezium payload to get the WAL commit timestamp.

3. Silver MERGE throughput
   Rows/sec processed by the MERGE INTO job.
   Derived from Silver table metadata: total rows / (last_silver_updated - first_silver_updated).

All queries run against Trino (catalog=lakehouse) using the REST Iceberg catalog.
Trino is used here as the query tool, not as the benchmark subject.

Usage
-----
    python -m benchmarks.pipeline_benchmark
"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import trino

from benchmarks.config import get_trino_conn, load_config

RESULTS_DIR = Path(__file__).parent / "results"

BRONZE_TABLES = [
    "orders", "order_items", "customers",
    "products", "sellers", "order_payments", "order_reviews",
]


# ── Trino helper ────────────────────────────────────────────────────────────────

def trino_query(conn_params: dict, sql: str, catalog: str = "lakehouse", schema: str = "bronze") -> list:
    conn = trino.dbapi.connect(
        host=conn_params["host"],
        port=conn_params["port"],
        user="trino",
        catalog=catalog,
        schema=schema,
    )
    try:
        cur = conn.cursor()
        cur.execute(sql)
        return cur.fetchall()
    finally:
        conn.close()


# ── Measurement functions ───────────────────────────────────────────────────────

def measure_bronze_throughput(conn_params: dict) -> dict:
    """
    Per-table and aggregate Bronze ingestion throughput.
    throughput = row_count / (max_ingested_at - min_ingested_at) in seconds.
    """
    table_stats = []
    total_rows = 0

    for table in BRONZE_TABLES:
        rows = trino_query(conn_params, f"""
            SELECT
                COUNT(*)                                AS row_count,
                MIN(_cdc_ingested_at)                   AS first_ingested,
                MAX(_cdc_ingested_at)                   AS last_ingested
            FROM lakehouse.bronze.{table}
        """)
        row_count, first_ts, last_ts = rows[0]
        if row_count == 0 or first_ts is None or last_ts is None:
            continue

        duration_s = (last_ts - first_ts).total_seconds()
        rows_per_sec = round(row_count / max(duration_s, 1), 1)
        total_rows += row_count

        table_stats.append({
            "table": table,
            "row_count": row_count,
            "duration_s": round(duration_s, 1),
            "rows_per_sec": rows_per_sec,
        })
        print(f"  bronze.{table:20s}  {row_count:>8,} rows  {rows_per_sec:>8,.0f} rows/s")

    # Aggregate: total rows / full wall-clock span across all batches
    # Use the widest time window across all tables
    span_rows = trino_query(conn_params, """
        SELECT COUNT(*), MIN(_cdc_ingested_at), MAX(_cdc_ingested_at)
        FROM (
            SELECT _cdc_ingested_at FROM lakehouse.bronze.orders          UNION ALL
            SELECT _cdc_ingested_at FROM lakehouse.bronze.order_items      UNION ALL
            SELECT _cdc_ingested_at FROM lakehouse.bronze.customers        UNION ALL
            SELECT _cdc_ingested_at FROM lakehouse.bronze.products         UNION ALL
            SELECT _cdc_ingested_at FROM lakehouse.bronze.sellers          UNION ALL
            SELECT _cdc_ingested_at FROM lakehouse.bronze.order_payments   UNION ALL
            SELECT _cdc_ingested_at FROM lakehouse.bronze.order_reviews
        ) t
    """)
    total, first_all, last_all = span_rows[0]
    span_s = (last_all - first_all).total_seconds() if first_all and last_all else 1
    aggregate_rows_per_sec = round(total / max(span_s, 1), 1)

    print(f"\n  TOTAL  {total:>8,} rows  {aggregate_rows_per_sec:>8,.0f} rows/s  "
          f"(span: {span_s:.0f}s)")

    return {
        "per_table": table_stats,
        "total_rows": total,
        "total_span_s": round(span_s, 1),
        "aggregate_rows_per_sec": aggregate_rows_per_sec,
    }


def measure_e2e_latency(conn_params: dict) -> dict:
    """
    E2E latency: PostgreSQL WAL commit → Iceberg write.
    Uses the orders table (largest, most representative) for percentile calculation.
    _cdc_source_ts_ms is the PostgreSQL WAL commit timestamp (epoch ms, from Debezium).
    _cdc_ingested_at is the Spark micro-batch write timestamp.
    """
    # Trino: to_unixtime(cast(ts AS TIMESTAMP WITH TIME ZONE)) for epoch seconds
    rows = trino_query(conn_params, """
        SELECT
            approx_percentile(
                to_unixtime(cast(_cdc_ingested_at AS TIMESTAMP WITH TIME ZONE))
                - _cdc_source_ts_ms / 1000.0,
                0.50
            ) AS p50_s,
            approx_percentile(
                to_unixtime(cast(_cdc_ingested_at AS TIMESTAMP WITH TIME ZONE))
                - _cdc_source_ts_ms / 1000.0,
                0.95
            ) AS p95_s,
            approx_percentile(
                to_unixtime(cast(_cdc_ingested_at AS TIMESTAMP WITH TIME ZONE))
                - _cdc_source_ts_ms / 1000.0,
                0.99
            ) AS p99_s,
            COUNT(*) AS measured_rows
        FROM lakehouse.bronze.orders
        WHERE _cdc_source_ts_ms IS NOT NULL
          AND _cdc_source_ts_ms > 0
    """)
    p50, p95, p99, measured = rows[0]
    result = {
        "p50_s": round(float(p50), 2) if p50 else None,
        "p95_s": round(float(p95), 2) if p95 else None,
        "p99_s": round(float(p99), 2) if p99 else None,
        "measured_rows": measured,
    }
    print(f"  P50={result['p50_s']}s  P95={result['p95_s']}s  P99={result['p99_s']}s"
          f"  (n={measured:,})")
    return result


def measure_silver_throughput(conn_params: dict) -> dict:
    """
    Silver MERGE throughput per table and aggregate.
    Uses _silver_updated_at to compute the processing window.
    """
    table_stats = []
    total_rows = 0

    for table in BRONZE_TABLES:
        rows = trino_query(conn_params, f"""
            SELECT
                COUNT(*)                                AS row_count,
                MIN(_silver_updated_at)                 AS first_updated,
                MAX(_silver_updated_at)                 AS last_updated
            FROM lakehouse.silver.{table}
        """, schema="silver")
        row_count, first_ts, last_ts = rows[0]
        if row_count == 0 or first_ts is None or last_ts is None:
            continue

        duration_s = max((last_ts - first_ts).total_seconds(), 1)
        rows_per_sec = round(row_count / duration_s, 1)
        total_rows += row_count

        table_stats.append({
            "table": table,
            "row_count": row_count,
            "duration_s": round(duration_s, 1),
            "rows_per_sec": rows_per_sec,
        })
        print(f"  silver.{table:20s}  {row_count:>8,} rows  {rows_per_sec:>8,.0f} rows/s")

    return {
        "per_table": table_stats,
        "total_rows": total_rows,
    }


# ── Entry point ─────────────────────────────────────────────────────────────────

def main() -> None:
    cfg = load_config()
    conn = get_trino_conn(cfg)

    print("\n── Bronze ingestion throughput ──")
    bronze = measure_bronze_throughput(conn)

    print("\n── E2E commit-to-Bronze latency (PostgreSQL WAL → Iceberg write) ──")
    e2e = measure_e2e_latency(conn)

    print("\n── Silver MERGE throughput ──")
    silver = measure_silver_throughput(conn)

    output = {
        "date": date.today().isoformat(),
        "bronze_throughput": bronze,
        "e2e_latency": e2e,
        "silver_throughput": silver,
    }

    RESULTS_DIR.mkdir(exist_ok=True)
    out = RESULTS_DIR / f"{date.today().isoformat()}-pipeline.json"
    out.write_text(json.dumps(output, indent=2))
    print(f"\nResults → {out}")
    print("Run 'python -m benchmarks.report' to include in the full benchmark report.")


if __name__ == "__main__":
    main()
