# ADR-007: StarRocks Replaces Trino as the Gold Query Layer

**Status:** Accepted  
**Date:** 2026-05-25

## Context

The original stack used Trino as the SQL query engine for Gold and dbt-trino for transformations. Two problems prompted this change:

1. **Query performance for Gold**: Trino is a federated query engine optimised for ad-hoc queries across many heterogeneous sources. The Gold layer is a fixed, aggregation-heavy workload queried by the FastAPI text-to-SQL agent. StarRocks' vectorised MPP engine is purpose-built for this type of aggregation workload.

2. **dbt compatibility**: dbt-starrocks supports async materialized views natively. dbt-trino with Iceberg incremental models has known edge cases. StarRocks MVs provide transparent query rewriting — the agent can query Silver directly and StarRocks rewrites it to hit the Gold MV automatically.

## Decision

Deploy StarRocks in shared-data mode as the Gold query engine. Trino remains deployed temporarily alongside StarRocks for the benchmark phase (Weeks 4-6), then is removed. All Gold data is stored as StarRocks async materialized views in MinIO (`lakehouse-starrocks/`). Silver data stays in Iceberg on MinIO and is never copied into StarRocks — StarRocks reads it via an external Iceberg catalog backed by the REST catalog.

## Benchmark Results

**Run date:** 2026-05-29  
**Protocol:** ClickBench-inspired. Cold cache (StarRocks `ALTER SYSTEM DROP ALL CACHE` + Trino worker restart), then 2 warmup runs + 3 timed runs. Metric = geometric mean of the 3 timed runs. See [docs/benchmarks/methodology.md](../benchmarks/methodology.md) for full details.

Three scenarios:

| Scenario | Description |
|----------|-------------|
| `trino_scan` | Trino 435 — full Parquet scan via REST catalog + MinIO (file-system cache enabled) |
| `starrocks_scan` | StarRocks 3.4 CN — same Iceberg files via `iceberg_catalog` (CN block cache) |
| `starrocks_mv` | StarRocks 3.4 CN — pre-materialised Gold MVs, no Iceberg scan |

**Warm query performance (geometric mean of 3 timed runs):**

| Query | Trino scan | StarRocks scan | StarRocks Gold MV | MV speedup vs Trino |
|-------|:----------:|:--------------:|:-----------------:|:-------------------:|
| Q01 daily revenue by category | 11.1 s | 525 ms | 62 ms | **178.9×** |
| Q02 top sellers by GMV | 5.81 s | 661 ms | 38 ms | **154.4×** |
| Q03 review sentiment by state | 5.60 s | 41 ms | 25 ms | **225.9×** |
| Q04 order funnel | 6.87 s | 304 ms | 23 ms | **295.1×** |
| Q05 cohort retention (90d) | 5.56 s | 208 ms | n/a (no MV) | — |
| **Geo-mean** | **6.72 s** | **246 ms** | **34 ms** | **197.5×** |

StarRocks scan is **27.4× faster** than Trino on the same Silver Iceberg files. Gold MVs push the gap to **197.5×** vs Trino, bringing P50 agent query latency to sub-40 ms.

**Pipeline metrics:**

| Metric | Value |
|--------|-------|
| Total Bronze rows (7 tables) | 812,193 |
| Bronze ingestion throughput | 554 rows/s (aggregate) |
| E2E latency P50 (WAL commit → Iceberg write) | 6,825 s ¹ |
| E2E latency P95 | 6,825 s ¹ |
| Silver MERGE throughput | ~100,000+ rows/s per table |

¹ High E2E latency reflects batch-loaded historical data (CSVs loaded into PostgreSQL ~1.9 hours before the Debezium + Spark pipeline ingested them). In a live production CDC deployment where Debezium reads the WAL in real-time, P50 would be sub-30 s.

Hardware: k3d 4-node cluster, Apple M2 Pro, 16 GB RAM. Dataset: Brazilian Olist (~812,000 rows across 7 tables).

## Consequences

**Good:** StarRocks shared-data mode is stateless at the compute layer (CN pods) — no persistent volumes for query data, fits k3d's local storage constraints. MV refresh is partition-aware and near-instant for incremental changes. The external Iceberg catalog means Silver data is never duplicated.

**Bad:** StarRocks adds operational complexity (FE + CN pods, storage volume initialisation). Trino is retained temporarily, increasing cluster resource usage during the benchmark phase.

**Trino removal:** After benchmark results are documented here, run `make remove-trino` to free cluster resources.

## Alternatives Considered

**Keep Trino:** Simpler operationally but slower for aggregation workloads. Would require dbt-trino's incremental model support, which has known Iceberg edge cases.

**Clickhouse:** Strong OLAP performance but less mature Iceberg connector than StarRocks. StarRocks 3.x has a production-ready Iceberg external catalog.
