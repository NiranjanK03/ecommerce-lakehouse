# ACID Guarantees and Query Speed Are Not a Tradeoff. Here's Proof.

---

## The Problem I Was Trying to Solve

We were designing an architecture where compute and storage are completely separated — the goal being faster analytics for the analysts and data scientists who consume our data. The conversation kept coming back to Iceberg. Everyone wanted it. And for good reason — ACID transactions, schema evolution, time travel, multi-engine support. In the real world, you cannot build a data platform analysts trust without ACID. Half-committed data showing up in a dashboard is not a performance problem. It is a trust problem.

But Iceberg alone does not make queries fast. It makes data reliable. Those are two different problems, and I kept seeing them conflated in every architecture discussion I was part of.

I wanted to know what the real difference looks like when you solve both — with actual numbers, not vendor benchmarks on static TPC-DS data. So I built a simulation on my MacBook.

---

## What I Built

A full CDC pipeline from PostgreSQL to queryable Gold analytics, running entirely on local Kubernetes:

```
PostgreSQL (Olist e-commerce dataset)
      │
      │  Debezium reads the WAL
      ▼
    Kafka
      │
      │  Spark Structured Streaming
      ▼
  Bronze  — raw CDC events, append-only Iceberg tables
      │
      │  Spark batch MERGE INTO
      ▼
  Silver  — deduplicated, typed, current-state Iceberg tables
      │
      │  dbt-StarRocks async materialized views
      ▼
  Gold  — pre-aggregated results in StarRocks shared-data
```

The dataset is the Brazilian Olist e-commerce dataset — 5 million rows across 7 tables (orders, customers, products, sellers, payments, reviews, order items). A realistic relational schema with foreign keys, nulls, and timestamps. Loading it into PostgreSQL and pointing Debezium at it gives you a live CDC source that behaves exactly like a production database.

Everything runs on k3d — Kubernetes in Docker, four nodes. MinIO as S3-compatible object storage. An Iceberg REST catalog tracking all table metadata. Airflow triggering Silver jobs only when new Bronze snapshots land, not on a fixed schedule.

---

## Three Layers, Three Guarantees

Most architecture articles describe layers. I want to describe what each layer actually guarantees to the person consuming the data.

**Bronze** is append-only and never modified. Every CDC event lands here exactly as Debezium published it — raw JSON, before and after row images, the WAL commit timestamp. If anything downstream breaks, Bronze is the source of truth to replay from. Your data scientists can trust that nothing was dropped or transformed.

**Silver** has one current-state row per primary key. Deletes are soft — `is_deleted = true` — so downstream joins do not lose history. The MERGE INTO semantics are idempotent: running the same job twice produces the same Silver state. Your analysts build on Silver knowing it is consistent and complete.

**Gold** is pre-aggregated business metrics in StarRocks materialized views. No Iceberg scan on query time. The results are already computed and sitting in StarRocks' own storage. Your analysts' dashboards load before they notice.

This is what solving both problems looks like architecturally. ACID lives at the Iceberg layer. Speed lives at the StarRocks layer. Neither compromises the other.

---

## The Key Decisions

### Iceberg for ACID — not for speed

Iceberg gives you snapshot isolation, atomic commits, schema evolution without rewriting files, and a REST catalog spec that any engine can read. Trino reads it. StarRocks reads it. Spark writes to it. There is one schema, one source of truth, and no synchronisation logic between engines.

What Iceberg does not give you is fast queries. Reading Parquet files from object storage for every analytical query has a cost. That cost is the baseline the benchmark measures.

### StarRocks for speed — not as an Iceberg replacement

StarRocks in shared-data mode has stateless compute nodes and C++ vectorised execution — no JVM on the query path. More importantly, it reads Silver Iceberg tables via an external catalog backed by the same REST catalog Spark writes to. StarRocks does not copy or replace the Silver data. It reads the same Parquet files and caches Parquet pages in CN memory after the first read.

For Gold materialized views, StarRocks goes further — it pre-aggregates the results into its own storage so repeated queries skip the Iceberg scan entirely.

This is the architecture insight that most benchmark articles miss: StarRocks and Iceberg are not competing. StarRocks sits on top of Iceberg. Iceberg provides the guarantees. StarRocks provides the speed.

### Debezium for true end-to-end latency

Debezium reads the PostgreSQL WAL directly. Every CDC event carries the WAL commit timestamp — the actual moment the transaction committed in the source database, not when Kafka received it or when Spark processed it. This is what makes the pipeline benchmark meaningful. You can compute true end-to-end latency: time from the moment data changed in PostgreSQL to the moment it was queryable in Iceberg.

Tools that use full or incremental table scans cannot give you this. They never see the WAL commit timestamp.

### dbt-StarRocks for Gold transforms

The Gold layer is four materialized views — daily revenue by product category, top sellers by GMV, review sentiment by Brazilian state, and a weekly order funnel. Each one is a SELECT against Silver Iceberg tables via StarRocks' external catalog. dbt-StarRocks translates `materialized='materialized_view'` with `refresh_async=True` into the correct StarRocks DDL. StarRocks refreshes the views automatically when Silver Iceberg snapshots change — no Airflow trigger needed for Gold.

---

## The Bugs That Actually Taught Me Things

**Debezium timestamp encoding.** PostgreSQL TIMESTAMP columns come through Debezium as epoch microseconds — a plain integer like `1537551000000000`. Spark's direct cast to TimestampType returns NULL. Silently. No error in the logs. The pipeline looks healthy. You find it only by sampling actual Silver data and noticing every timestamp column is NULL. The fix is dividing by 1,000,000 first and going through `timestamp_seconds()`. The lesson: Debezium's numeric type encoding is not prominently documented and Spark's default behaviour for failed casts is silent NULL, not an exception.

**Iceberg TIMESTAMPTZ vs StarRocks.** Spark 3.5 writes timestamp columns to Iceberg as `timestamptz` by default. StarRocks 3.4's external Iceberg catalog cannot decode `timestamptz` and returns NULL. The fix is writing `TIMESTAMP_NTZ` in Silver DDL. One keyword. Three hours to diagnose because everything upstream looked correct — the timestamps were right in Trino, right in the Iceberg metadata, and NULL only in StarRocks.

**Iceberg catalog OOM under concurrent snapshots.** The Iceberg REST catalog pod was being killed during Silver MERGE jobs when multiple tables committed snapshots within 45 seconds of each other. The root cause was not memory alone — the Kubernetes liveness probe's `failureThreshold` was set to 3 with a 15-second period, giving a total tolerance of 45 seconds. A JVM GC pause looks like a hung process and the probe kills the pod. Increasing memory alone did not fix it. The `failureThreshold` had to go up too. The lesson: when a JVM-based service dies under load in Kubernetes, check the liveness probe configuration before assuming the service is broken.

---

## The Benchmark

I ran five analytical queries across three scenarios on the same Silver Iceberg data:

- Q01: Daily revenue by product category — 3-table join, GROUP BY date, SUM
- Q02: Top sellers by GMV — 3-table join, conditional aggregation, ORDER + LIMIT
- Q03: Review sentiment by customer state — multi-bucket conditional SUM
- Q04: Weekly order funnel — DATE_TRUNC, 6 conditional COUNTs
- Q05: 90-day cohort retention — CTE + self-join + INTERVAL date arithmetic

**Three scenarios:**

`trino_scan` — Trino 435 reads Silver Iceberg Parquet files via the REST catalog directly from MinIO.

`starrocks_scan` — StarRocks 3.4 reads the same Parquet files via its external Iceberg catalog. CN block cache enabled.

`starrocks_mv` — StarRocks queries pre-materialised Gold views. No Iceberg scan. No Parquet decode. No MinIO round-trip.

**Protocol:** Cold cache first — StarRocks block cache dropped, Trino worker pod restarted. Then 2 warmup runs followed by 3 timed runs. Metric is geometric mean of the 3 timed runs. Geometric mean because one fast query should not inflate the aggregate — the same approach ClickBench uses.

**Results (warm cache, geometric mean of 3 timed runs, 5M-row dataset):**

| Scenario | Geo-mean latency | vs Trino |
|----------|:---:|:---:|
| Trino 435 — Silver Iceberg scan | 12,938 ms | baseline |
| StarRocks 3.4 — Silver Iceberg scan + CN cache | 1,318 ms | **9.8× faster** |
| StarRocks 3.4 — Gold materialized views | 47 ms | **273× faster** |

**Important caveat:** this runs entirely on a single MacBook. StarRocks and Trino share the same CPU and memory. In a real deployment with dedicated nodes the gap would be wider, not narrower. The numbers are conservative.

**The pipeline metric you cannot get from vendor benchmarks:**

Standard benchmarks measure query execution on static data. I also measured end-to-end latency — time from PostgreSQL WAL commit to Iceberg write — using `source.ts_ms` embedded by Debezium in every CDC event. This is the only metric that captures true source-to-storage lag. The bulk snapshot ingested 5,071,289 rows at 11,406 rows/s. In a live CDC deployment reading the WAL in real-time, P50 latency would be sub-30 seconds.

---

## What This Means in Practice

A 6-second query is not a performance problem for an analyst. It is a behaviour change. Analysts stop clicking. They screenshot yesterday's number instead of pulling today's. They ask the data team for a CSV. The dashboard becomes a liability instead of an asset.

47ms is invisible. The analyst does not experience it as a load time. The dashboard just works.

The 273× gap between Trino and StarRocks Gold MVs is not about the tools. It is about recognising that ACID correctness and query speed are separate problems that require separate solutions — and that building for one does not mean compromising on the other.

---

## The Part Nobody Writes About

Pre-materialisation only works if you know what to pre-materialise. The Gold MVs in this project cover four query patterns because those are the most common analytical questions on an e-commerce dataset. In production, your query patterns are whatever your analysts actually run — and at a startup that changes every quarter as the product evolves.

Tools exist to help with this: query log analysis, data observability platforms, usage tracking in dbt. The engineering is not the hard part. The hard part is building the feedback loop between the analysts who consume data and the engineers who build the pipeline — getting that information flowing before users stop trusting the platform, not after.

That is the problem no architecture diagram captures. And it is the one worth solving first.

---

## Repo

Full code, benchmark queries, methodology, and results:
**github.com/NiranjanK03/ecommerce-lakehouse**
