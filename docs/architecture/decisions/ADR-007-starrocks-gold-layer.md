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

**Warm query performance (geometric mean of 3 timed runs, 5M-row dataset, 2026-09-11):**

| Query | Trino scan | StarRocks scan | StarRocks Gold MV | MV speedup vs Trino |
|-------|:----------:|:--------------:|:-----------------:|:-------------------:|
| Q01 daily revenue by category | 17.4 s | 2.97 s | 54 ms | **322.0×** |
| Q02 top sellers by GMV | 20.2 s | 3.49 s | 44 ms | **453.3×** |
| Q03 review sentiment by state | 8.74 s | 55 ms | 44 ms | **199.6×** |
| Q04 order funnel | 10.4 s | 1.67 s | 48 ms | **213.6×** |
| Q05 cohort retention (90d) | 11.4 s | 4.16 s | n/a (no MV) | — |
| **Geo-mean** | **12.94 s** | **1.32 s** | **47 ms** | **273.3×** |

StarRocks scan is **9.8× faster** than Trino on the same Silver Iceberg files. Gold MVs push the gap to **273.3×** vs Trino, bringing agent query latency to sub-55 ms.

**Pipeline metrics (5M-row synthetic dataset):**

| Metric | Value |
|--------|-------|
| Total Bronze rows (7 tables) | 5,071,289 |
| Bronze ingestion throughput | 11,406 rows/s (aggregate, bulk snapshot) |
| E2E latency P50 (WAL commit → Iceberg write) | 0.02 s ¹ |
| Silver MERGE throughput | ~11,400 rows/s aggregate over 7 tables |

¹ Bulk-loaded synthetic data (source timestamp = ingest timestamp). In a live Debezium CDC deployment reading the PostgreSQL WAL in real-time, P50 would be sub-30 s.

Hardware: k3d 4-node cluster, Apple M2 Pro, 16 GB RAM. Dataset: Brazilian Olist synthetic (~5M rows across 7 tables).

## Consequences

**Good:** StarRocks shared-data mode is stateless at the compute layer (CN pods) — no persistent volumes for query data, fits k3d's local storage constraints. MV refresh is partition-aware and near-instant for incremental changes. The external Iceberg catalog means Silver data is never duplicated.

**Bad:** StarRocks adds operational complexity (FE + CN pods, storage volume initialisation). Trino is retained temporarily, increasing cluster resource usage during the benchmark phase.

**Trino removal:** After benchmark results are documented here, run `make remove-trino` to free cluster resources.

## Alternatives Considered

**Keep Trino:** Simpler operationally but slower for aggregation workloads. Would require dbt-trino's incremental model support, which has known Iceberg edge cases.

**Clickhouse:** Strong OLAP performance but less mature Iceberg connector than StarRocks. StarRocks 3.x has a production-ready Iceberg external catalog.
