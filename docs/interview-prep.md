# Interview Prep — ecommerce-lakehouse

This guide helps you talk confidently about every part of the project in an interview. Sections are ordered by how often each topic comes up.

---

## Elevator pitch

**30 seconds:**
> "I built a fully open-source data lakehouse on local Kubernetes using the Olist e-commerce dataset. It runs a complete CDC pipeline — PostgreSQL changes flow through Debezium and Kafka into Bronze Iceberg tables, get cleaned and deduplicated into Silver via PySpark MERGE INTO, and land in StarRocks as pre-materialised Gold views. I benchmarked StarRocks against Trino on the same data and measured the full pipeline latency from WAL commit to queryable Gold."

**2 minutes (adds the why):**
> "The main goal was to demonstrate that you can build a production-grade lakehouse on open-source tools without any vendor lock-in. The storage adapter pattern means the whole thing swaps between MinIO, S3, and ADLS by changing one config line.
>
> The interesting technical problems were: making CDC-to-Iceberg exactly-once at the right granularity (Debezium offset tracking + Spark micro-batch checkpointing), handling Iceberg's reserved column names and lack of DEFAULT support in DDL, tuning the Iceberg REST catalog for concurrent MERGE INTO workloads, and designing a benchmark that proves value — not just 'StarRocks is faster' but by how much, on what query shapes, and why.
>
> The benchmark has three layers: pipeline throughput (rows/s from PostgreSQL commit to Iceberg), E2E latency using the WAL commit timestamp embedded in the Debezium envelope, and a ClickBench-inspired cold/warm query comparison across three scenarios: Trino scan, StarRocks scan via external Iceberg catalog, and StarRocks Gold materialized views."

---

## Architecture decisions — the "why"

### Why Iceberg instead of Delta Lake or Hudi?

Iceberg was chosen because:
- **Vendor-neutral spec** — no Databricks dependency, no Confluent dependency
- **REST catalog spec** — any catalog (Polaris, Nessie, Iceberg REST, Unity) can be swapped in without changing Spark or query engine config
- **Native support** from all three engines in this stack: Spark (native Iceberg), Trino (native Iceberg connector), StarRocks (external Iceberg catalog)
- **MERGE INTO semantics** that map cleanly to CDC upsert/delete patterns

Delta Lake requires a Spark runtime with the Delta JAR and ties you to the Delta transaction log format. Hudi has good CDC support but the catalog story is weaker for multi-engine setups.

### Why StarRocks for Gold instead of keeping Trino?

Trino is a federated query engine — it shines when you need to join across Hive, TPCH, Delta, and Iceberg in a single query. It is JVM-based, which adds GC overhead for tight analytical loops.

StarRocks is a purpose-built MPP engine with:
- **C++ vectorised execution** — no JVM GC on the query path
- **Shared-data mode** — CN pods are stateless; no PVCs for query data; fits k3d's storage constraints
- **CN block cache** — Parquet pages cached in CN memory after first read; repeated queries skip MinIO entirely
- **Async materialized views** — dbt-StarRocks builds them with `materialized='materialized_view'` and `refresh_async=True`; StarRocks refreshes them automatically when Silver snapshots change

The benchmark quantifies this. Both engines read the same Parquet files from MinIO via the same REST catalog. The difference is the engine.

### Why Debezium instead of Airbyte or Fivetran?

Debezium reads the PostgreSQL WAL directly and publishes per-row change events with:
- Exact operation type (INSERT / UPDATE / DELETE)
- Before + after row images
- WAL commit timestamp (`source.ts_ms`) — which the benchmark uses to compute E2E latency

Airbyte and Fivetran use full/incremental table scans, not WAL-based CDC. They cannot capture deletes without soft-delete columns, and they cannot give you the sub-second WAL commit timestamp needed to compute true E2E latency.

### Why k3d instead of minikube or kind?

k3d runs k3s in Docker, which means it uses the same container runtime as production EKS/GKE/AKS. The SparkApplication CRDs, Helm releases, and ConfigMaps work identically on real Kubernetes. Minikube uses a VM; kind is comparable but k3d has better support for multi-node clusters and built-in local registry.

### Why not use a managed service (Databricks, Confluent, Snowflake)?

Deliberate constraint. The project demonstrates that the same outcomes are achievable with open-source tools. The storage adapter pattern (`config/local.yaml` → `config/aws.yaml`) means migrating to S3 is a config change, not a rewrite. This is a stronger portfolio signal than "I used Databricks" because it shows you understand what each component does.

---

## Technical challenges you solved

These are the problems worth talking about in detail. Each one has a clear problem, root cause, and fix.

### 1. Iceberg reserved column name `_deleted`

**Problem:** Silver MERGE INTO failed with `ValidationException: Table column names conflict with names reserved for Iceberg metadata columns: [_deleted]`.

**Root cause:** Iceberg 1.5 uses `_deleted` internally for row-level delete files. User columns cannot shadow it.

**Fix:** Renamed the soft-delete flag from `_deleted` to `is_deleted` across the Silver DDL, the MERGE INTO job, all 4 Gold dbt models, and all 5 benchmark queries.

**Why it matters in an interview:** Shows you understand the distinction between the Iceberg table format spec and the user-visible schema. It is not a Spark bug — it is Iceberg enforcing a reserved namespace.

### 2. Iceberg catalog OOM under concurrent MERGE snapshots

**Problem:** Silver MERGE jobs succeeded locally but the Iceberg REST catalog pod was killed (exit code 143 — SIGTERM from the liveness probe) when multiple tables committed snapshots within 45 seconds of each other.

**Root cause:** The catalog had a 512 Mi memory limit. Under concurrent Iceberg snapshot commits, GC stalls exceeded the liveness probe's default tolerance (3 × 15s = 45s). The liveness probe treated the GC pause as a hung process and killed the pod.

**Fix:** Increased memory limit to 1 Gi and `livenessProbe.failureThreshold` from 3 to 10 (150s tolerance). The catalog is a single-instance Java process — GC pauses are expected; the probe threshold must accommodate them.

**Why it matters:** Shows you can debug Kubernetes liveness/readiness probe interactions with JVM GC, not just "it crashed."

### 3. `s3://` vs `s3a://` path routing in Spark

**Problem:** Silver SparkApplication failed to read Iceberg table locations even though the Bronze jobs worked. The Iceberg REST catalog stores locations as `s3://lakehouse-warehouse/silver/...` (matching the warehouse URI prefix). Spark's Hadoop FileSystem routing did not know to use the S3A connector for `s3://` URIs.

**Fix:** Added `spark.hadoop.fs.s3.impl = org.apache.hadoop.fs.s3a.S3AFileSystem` to the SparkSession config and SparkApplication CRD `sparkConf`. This forces all `s3://` paths through S3A, which is the correct connector for MinIO (and AWS S3 in production).

**Why it matters:** This is a common footgun in self-managed Spark + Iceberg setups. The fix is one line, but diagnosing it requires understanding that Hadoop has separate `fs.s3.*` and `fs.s3a.*` implementations.

### 4. Cross-engine SQL compatibility for cohort retention

**Problem:** Q05 (cohort retention) used `DATEDIFF(end, start)` which is valid StarRocks syntax (MySQL dialect) but not Trino syntax. Trino uses `date_diff('day', start, end)`. A single SQL file that runs on both engines was needed.

**Fix:** Rewrote Q05 using `INTERVAL '30' DAY` arithmetic in the JOIN ON clause and CASE expressions. Both Trino and StarRocks implement ANSI standard INTERVAL arithmetic. The rewrite is also semantically cleaner — filtering by `purchased_at <= first_purchase_at + INTERVAL '30' DAY` is more readable than `DATEDIFF(purchased_at, first_purchase_at) <= 30`.

**Why it matters:** Shows awareness of SQL dialect differences and the ANSI standard as a cross-engine portability layer.

### 5. Debezium timestamp encoding — epoch microseconds, not strings

**Problem:** All Silver timestamp columns (`purchased_at`, `approved_at`, etc.) were NULL in Silver Iceberg and consequently in StarRocks Gold MVs. The Gold `daily_revenue` MV had all-NULL order dates; `order_funnel` collapsed to a single row covering all 99,441 orders. No errors appeared in Spark job logs.

**Root cause:** Two issues stacked on top of each other:

1. Debezium encodes PostgreSQL `TIMESTAMP` columns as **epoch microseconds** (a plain integer) in the CDC JSON payload — not as an ISO string like `"2018-09-21 10:56:33"`. The Silver merge was doing `.cast(TimestampNTZType())` on the string `"1537551000000000"`, which Spark cannot parse as a timestamp and silently returns NULL.

2. Spark 3.5 writes all `TIMESTAMP` columns to Iceberg as `timestamptz` (with timezone). StarRocks 3.4's Iceberg external catalog cannot decode `timestamptz` and returns NULL — even when the underlying values are correct.

**Fix (two-part):**
- Changed Silver DDL: `TIMESTAMP` → `TIMESTAMP_NTZ` so Iceberg stores timezone-naive timestamps that StarRocks can read.
- Changed Silver merge extraction: instead of `.cast(TimestampNTZType())` directly, divide epoch microseconds by 1,000,000 first: `(raw.cast("long") / 1_000_000).cast("timestamp_ntz")`. For `DATE` columns (Debezium encodes as epoch days): `to_date(from_unixtime(raw.cast("long") * 86400))`.

**Why it matters:** This is the most common silent data quality bug in CDC pipelines. Debezium's numeric encoding of temporal types is not prominently documented. There is no error — Spark silently returns NULL for failed casts, so the pipeline looks healthy in logs. Discovery requires sampling actual Silver data. The fix requires understanding three layers simultaneously: Debezium's JSON encoding, Spark's type system, and the Iceberg type schema.

### 6. Benchmark design — why E2E latency is unique

Standard database benchmarks (ClickBench, TPC-H) measure query execution time on static data. They cannot measure the latency of the data pipeline feeding those queries.

This project's E2E latency metric is computed as:

```
latency = _cdc_ingested_at - (_cdc_source_ts_ms / 1000)
```

where `_cdc_source_ts_ms` is the PostgreSQL WAL commit timestamp embedded by Debezium in the CDC envelope — not a synthetic timestamp added during ingestion. Infrastructure-level metrics (Kafka consumer lag, Debezium connector lag) cannot compute this because they only see byte offsets, not the original WAL commit time.

P50/P95/P99 are computed with Trino's `approx_percentile` over the full Bronze orders table (~99k rows), which gives statistically meaningful latency distribution without reading all rows into the application tier.

---

## Metrics to cite

Benchmarks run 2026-05-29. Full report: `benchmarks/results/2026-05-29-benchmark-report.md`.

| Metric | Value |
|--------|-------|
| Total Bronze rows (7 tables) | 812,193 |
| Bronze ingestion throughput | 554 rows/s (aggregate over 7 tables, 1,466 s span) |
| E2E latency P50 (WAL commit → Iceberg write) | 6,825 s ¹ |
| E2E latency P95 | 6,825 s ¹ |
| Trino warm geo-mean (5 queries) | 6,720 ms |
| StarRocks scan warm geo-mean | 246 ms |
| StarRocks MV warm geo-mean (4 queries) | 34 ms |
| StarRocks scan speedup vs Trino | 27.4× |
| StarRocks MV speedup vs Trino | 197.5× |

¹ Batch-loaded historical data: CSVs were loaded into PostgreSQL ~1.9 hours before Debezium + Spark ingested them. In live CDC, P50 would be sub-30 s.

---

## Common interview questions

**"Walk me through the architecture."**
Use the pipeline diagram from the README. Start at PostgreSQL (source of truth), walk through Debezium → Kafka → Spark Structured Streaming → Bronze Iceberg → Silver MERGE INTO → Gold StarRocks MVs → FastAPI agent. Emphasise: (1) the medallion layers and what each one guarantees, (2) the storage adapter pattern for cloud portability.

**"Why did you choose Iceberg over Delta Lake?"**
See "Architecture decisions" above. Key point: vendor neutrality + REST catalog spec + native multi-engine support.

**"How do you handle late-arriving data or duplicates?"**
The Silver MERGE INTO uses `_cdc_source_ts_ms` as the "source of truth" timestamp and deduplicates by primary key, keeping only the row with the highest `_cdc_source_ts_ms`. Late-arriving events with lower timestamps are discarded in the dedup window. This is a deliberate tradeoff: exactly-once at the Bronze level (Debezium offset tracking + Spark checkpointing), at-least-once at the Silver level with idempotent MERGE.

**"How do you handle schema evolution?"**
Iceberg supports schema evolution natively: add/rename/drop columns without rewriting existing Parquet files. The REST catalog tracks schema versions. In this project, schema changes propagate automatically to Trino and StarRocks because both engines read the Iceberg schema from the REST catalog, not from inferred file schema.

**"How would you scale this to production?"**
- Replace k3d with EKS/GKE/AKS
- Replace MinIO with S3 or ADLS (one config line change — storage adapter)
- Replace single Kafka broker with MSK or Confluent Cloud
- Scale Spark executor count and memory per job
- Scale StarRocks CN replicas horizontally (shared-data mode is stateless at CN)
- The Iceberg REST catalog can be replaced with AWS Glue, Polaris, or Nessie — the catalog type is one Spark config property

**"What would you do differently?"**
Honest answers (pick one or two):
- The MinIO bucket initialisation job has a race condition with the MinIO pod startup — worked around with a boto3 script rather than fixing the init container. In production I'd use a proper init container with exponential backoff.
- The benchmark runs on a single host shared between all components. For a more rigorous benchmark, StarRocks and Trino would have dedicated nodes so their resource consumption doesn't affect each other.
- Q05 cohort retention cannot be pre-materialised as an MV — it requires a self-join with a sliding INTERVAL window. A production-grade solution would use a periodic batch job that materialises cohort tables on a schedule.

**"Describe a bug that was hard to debug."**
Use the Iceberg catalog OOM issue (see Technical challenges #2). It checks multiple boxes: it involved Kubernetes (liveness probes), JVM internals (GC), and the Iceberg catalog's concurrency model. The fix was non-obvious: the memory increase alone was not sufficient — the `failureThreshold` also had to be raised, because a GC pause of even 60s would kill the pod even with more heap.

---

## Demo walkthrough (if asked to show the project)

1. Show `make status` — all pods Running across 6 namespaces
2. Open MinIO console (port 9001) — show `lakehouse-warehouse/bronze/` and `lakehouse-warehouse/silver/` Parquet files
3. Open Trino UI (port 8085) — run `SHOW TABLES IN lakehouse.silver`
4. Run Q01 on Trino: `SELECT * FROM lakehouse.silver.orders LIMIT 5`
5. Open StarRocks via MySQL client — `SHOW MATERIALIZED VIEWS IN gold`
6. Run a Gold MV query — show sub-100ms response
7. Show `benchmarks/results/YYYY-MM-DD-benchmark-report.md` — the 6-section report
8. Point to `docs/benchmarks/methodology.md` — shows engineering rigour

---

## What makes this project stand out

- **Full pipeline, not just a query layer** — most portfolio lakehouses start at S3 and skip the CDC ingestion path entirely
- **Quantified claims** — E2E latency in seconds, not "fast"
- **Multi-engine benchmark with clear methodology** — ClickBench-inspired protocol, geometric mean, three scenarios that isolate different factors
- **Production-grade details** — Iceberg reserved column gotchas, liveness probe tuning, s3a routing — these are the problems that only appear in real deployments, not tutorials
- **Cloud-portable by design** — the storage adapter pattern means migrating to S3/ADLS is a config change, not a rewrite
