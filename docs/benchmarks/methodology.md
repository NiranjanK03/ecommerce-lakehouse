# Benchmark Methodology

This document explains how benchmarks were designed, why the protocol was chosen, and how to reproduce the results.

---

## Why this benchmark matters

Most publicly available database benchmarks (ClickBench, TPC-H, TPC-DS) measure query execution time on synthetic or static datasets. They tell you nothing about the latency of the data pipeline that feeds those queries.

This project runs a full CDC pipeline: PostgreSQL → Debezium → Kafka → Spark → Bronze (Iceberg) → Silver (Iceberg) → Gold (StarRocks). The benchmark captures the **entire path**, including:

- How fast raw events land in Iceberg after PostgreSQL commits them
- How long the Silver MERGE INTO job takes to process all 812,000 rows
- How the three serving engines compare on the same analytical queries

---

## Scenarios

Three scenarios are compared for SQL query performance:

| Scenario | Description |
|----------|-------------|
| `trino_scan` | Trino 435 queries Silver Iceberg via native S3 + REST catalog. File-system cache enabled (`/tmp/trino-cache`, 2 GB). |
| `starrocks_scan` | StarRocks 3.4 CN pods query the same Silver Iceberg via `iceberg_catalog`. Block cache enabled in shared-data mode. |
| `starrocks_mv` | StarRocks queries pre-materialized Gold MVs. No Iceberg scan — data lives in StarRocks native shared-data storage. |

The three scenarios isolate the contribution of each factor:

- **`trino_scan` vs `starrocks_scan`**: engine quality difference (JVM vs C++) on identical data
- **`starrocks_scan` cold vs warm**: CN block cache impact
- **`starrocks_scan` vs `starrocks_mv`**: pre-materialization impact

---

## Protocol

Protocol is inspired by [ClickBench](https://benchmark.clickhouse.com/) with adaptations for this environment.

### Cold phase

Before cold runs, both caches are fully evicted:

- **StarRocks**: `ALTER SYSTEM DROP ALL CACHE` (drops all CN block cache pages)
- **Trino**: `kubectl rollout restart deployment/trino-worker -n serving` (new pod starts with empty `/tmp/trino-cache`)

Each query is then run **once** and the wall-clock time recorded as `cold_ms`.

### Warm phase

After cold runs:

1. **2 warmup runs** — let caches fill, not recorded
2. **3 timed runs** — wall-clock time recorded per run
3. **Metric**: geometric mean of the 3 timed runs = `warm_geo_mean_ms`

### Gold MV phase

MVs live in StarRocks native storage — there is no Parquet cache to clear. They are always warm. The same warmup + timed protocol is used but without a cold run.

---

## Why geometric mean?

The geometric mean is the standard metric for multi-query benchmarks because it treats each query's speedup ratio equally regardless of absolute magnitude.

**Example:** If Q01 is 10× faster and Q05 is 1× faster on StarRocks:
- Arithmetic mean: (10 + 1) / 2 = **5.5×** (misleadingly high — Q01 dominates)
- Geometric mean: √(10 × 1) = **3.2×** (represents the typical per-query experience)

This is the same rationale used by ClickBench and TPC-H reporting.

---

## Pipeline metrics

### Bronze ingestion throughput

```
throughput = total_row_count / (max(_cdc_ingested_at) - min(_cdc_ingested_at))
```

`_cdc_ingested_at` is set by the Spark Structured Streaming job when it writes each Parquet file to MinIO. The time window is the observed wall-clock span of the ingestion job.

> **Note:** This is not the same as the Spark job's wall-clock duration because Spark writes multiple micro-batches and there may be idle periods between them. Using the actual timestamp range is more accurate.

### E2E commit-to-Bronze latency

```
latency = _cdc_ingested_at - (_cdc_source_ts_ms / 1000)
```

Where:
- `_cdc_source_ts_ms` — PostgreSQL WAL commit timestamp, epoch milliseconds, embedded by Debezium in the `source.ts_ms` envelope field. Captured in the Bronze table during ingestion.
- `_cdc_ingested_at` — timestamp of the Spark micro-batch write (set by the streaming job)

This metric is unique to this benchmark. Infrastructure-level metrics (Kafka lag, Debezium offset lag) cannot compute true source-to-storage latency because they do not have access to the original WAL commit timestamp — only the raw payload bytes.

P50 / P95 / P99 are computed using Trino's `approx_percentile` over the full Bronze orders table (largest table, most representative).

### Silver MERGE throughput

```
throughput = row_count / (max(_silver_updated_at) - min(_silver_updated_at))
```

`_silver_updated_at` is set by the PySpark Silver MERGE INTO job. Measured per table and reported as rows/second.

---

## Queries

Five queries are used for the scan comparison:

| ID | Query | Why |
|----|-------|-----|
| Q01 | Daily revenue by product category | Classic business aggregation — GROUP BY with SUM |
| Q02 | Top 50 sellers by GMV | Sort-heavy with join to sellers |
| Q03 | Review sentiment by customer state | JOIN across 3 tables, GROUP BY state |
| Q04 | Weekly order funnel | CASE aggregation + date truncation |
| Q05 | 90-day cohort retention | Self-join with INTERVAL window — cannot be pre-aggregated |

Q05 is intentionally included to test a query class that cannot be served by a materialized view. Both engines must handle it via full scan.

All queries are written in ANSI SQL using `INTERVAL '30' DAY` arithmetic (not `DATEDIFF`) to remain cross-engine compatible between Trino and StarRocks.

---

## Hardware

All tests run on a k3d 4-node cluster (1 server + 3 agents) on a single Apple M2 Pro host with 16 GB RAM.

**Constraint:** Because all components share the same physical host, the benchmark measures the relative performance of the query engines and caches under these resource constraints. It does **not** represent production-scale throughput — for that, a multi-node cluster with dedicated storage is needed.

**What this benchmark does measure accurately:**
- The relative advantage of the C++ engine (StarRocks) over the JVM engine (Trino) on the same hardware
- The cache speedup on repeated queries
- The pre-materialization speedup (scan vs MV)
- The full pipeline latency path (E2E, independent of query performance)

---

## Trino file-system cache configuration

Trino's catalog config (`infrastructure/k8s/helm/values/trino.yaml`):

```properties
fs.cache.enabled=true
fs.cache.max-size=2GB
fs.cache.base-directory=/tmp/trino-cache
```

This enables disk-based page caching for all Parquet files read through the lakehouse catalog. No JVM heap increase is needed — the cache is off-heap.

Cache is cleared by restarting the Trino worker pod (the new pod starts with an empty `/tmp/trino-cache` directory).

---

## Reproducing results

```bash
# Prerequisites: Silver tables populated, Trino + StarRocks deployed, Gold MVs built

# 1. Pipeline metrics (Bronze ingestion throughput + E2E latency + Silver MERGE)
python -m benchmarks.pipeline_benchmark

# 2. Engine comparison (cold + warm query timings, all 3 scenarios)
python -m benchmarks.engine_compare

# 3. Warm-only re-run (skip cache clear, faster)
python -m benchmarks.engine_compare --warm-only

# 4. Generate showcase report
python -m benchmarks.report

# Results written to benchmarks/results/
```

Raw timing data is stored as JSON in `benchmarks/results/`:
- `YYYY-MM-DD-pipeline.json` — pipeline metrics
- `YYYY-MM-DD-engine-compare.json` — per-query cold + warm timings
- `YYYY-MM-DD-benchmark-report.md` — generated showcase report
