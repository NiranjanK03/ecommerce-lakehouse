# ecommerce-lakehouse

A fully open-source, locally-runnable data lakehouse built on Kubernetes. Uses the Brazilian Olist e-commerce dataset as a realistic source of truth flowing through a complete CDC → Bronze → Silver → Gold pipeline.

```
PostgreSQL (Olist)
      │
      │  CDC via Debezium
      ▼
    Kafka
      │
      │  Spark Structured Streaming
      ▼
  Bronze (Iceberg / MinIO)   ← raw CDC events, append-only
      │
      │  Spark batch + MERGE INTO
      ▼
  Silver (Iceberg / MinIO)   ← deduplicated, typed, current state
      │
      │  dbt-StarRocks async materialized views
      ▼
   Gold (StarRocks shared-data)  ← business aggregates
```

## Stack

| Layer | Technology | Why |
|-------|-----------|-----|
| Local k8s | k3d | lightweight k3s in Docker, single binary |
| Storage | MinIO → S3/ADLS via adapter | local dev parity with cloud |
| Table format | Apache Iceberg | ACID, time-travel, schema evolution |
| Catalog | Iceberg REST Catalog | open REST spec, swappable backend |
| CDC | Debezium + Kafka | proven, offset-tracked, exactly-once capable |
| Message bus | Apache Kafka (Bitnami, KRaft) | no ZooKeeper, fast startup |
| Compute | Apache Spark (k8s operator) | native k8s scheduling, Iceberg native |
| SQL engine | StarRocks (shared-data) | vectorised MPP, compute/storage separated |
| Transforms | dbt-StarRocks | async materialized views on Silver Iceberg |
| Orchestration | Apache Airflow | KubernetesExecutor, event-driven Silver trigger |
| Observability | OTel Collector + Prometheus | metrics, traces |

## Benchmark Results

Run date: 2026-09-11 on a k3d 4-node cluster (Apple M2 Pro, 16 GB RAM). Full report: [`benchmarks/results/2026-09-11-benchmark-report.md`](benchmarks/results/2026-09-11-benchmark-report.md).

**Query engine comparison — 5 analytical queries, warm cache, geometric mean:**

| Engine | Geo-mean latency | Speedup vs Trino |
|--------|:----------------:|:----------------:|
| Trino 435 (Silver Iceberg scan) | 12,938 ms | baseline |
| StarRocks 3.4 (Silver Iceberg scan, CN cache) | 1,318 ms | **9.8×** |
| StarRocks 3.4 (Gold materialized views) | 47 ms | **273.3×** |

**Pipeline metrics:**

| Metric | Value |
|--------|-------|
| Total Bronze rows (7 tables) | 5,071,289 |
| Bronze ingestion throughput (bulk snapshot) | 11,406 rows/s |
| E2E latency P50 (WAL commit → Iceberg) | 0.02 s ¹ |

¹ Bulk-loaded synthetic dataset (source timestamp = ingest timestamp). In live Debezium CDC the WAL-to-Iceberg lag is sub-30 s.

See [docs/architecture/decisions/ADR-007-starrocks-gold-layer.md](docs/architecture/decisions/ADR-007-starrocks-gold-layer.md) for the full analysis and [docs/benchmarks/methodology.md](docs/benchmarks/methodology.md) for the protocol.

## Storage Adapter

Switch between MinIO, S3, and ADLS by editing `config/<env>.yaml` — no application code changes needed. See [ADR-001](docs/architecture/decisions/ADR-001-storage-adapter.md).

## Quick Start

**Prerequisites:** Docker Desktop (≥ 8 GB RAM allocated), k3d, kubectl, helm

```bash
# 1. Clone and enter the repo
git clone https://github.com/niranjankulkarni/ecommerce-lakehouse.git
cd ecommerce-lakehouse

# 2. Create the k3d cluster + local registry
make cluster-up

# 3. Deploy core infrastructure
make deploy-infra

# 4. Forward all service ports to localhost
make port-forward

# 5. Verify everything is healthy
make status
```

See [docs/setup.md](docs/setup.md) for a detailed walkthrough and [PROJECT_MAP.md](PROJECT_MAP.md) for a directory guide.

## Directory Structure

```
.
├── infrastructure/k8s/
│   ├── cluster/          # k3d cluster config
│   ├── helm/values/      # Helm values per component
│   └── helm/charts/      # Custom k8s deployments (Iceberg, Debezium, etc.)
├── pipelines/
│   ├── spark/            # PySpark jobs + Docker image + SparkApplication CRDs
│   ├── airflow/          # DAGs + custom sensors
│   └── debezium/         # Connector configs + registration script
├── transform/dbt/        # dbt-StarRocks Gold materialized views
├── config/               # local.yaml (connection details, gitignored)
├── benchmarks/
│   ├── queries/          # SQL queries (scan path) + queries/mv/ (Gold MV path)
│   ├── results/          # JSON timing data + generated Markdown reports
│   ├── engine_compare.py # Cold + warm Trino vs StarRocks comparison
│   ├── pipeline_benchmark.py  # Ingestion throughput + E2E latency
│   └── report.py         # Generates full showcase report from results/
├── docs/
│   ├── architecture/decisions/  # ADRs
│   ├── benchmarks/              # methodology.md
│   └── runbooks/                # Per-component ops guides
└── scripts/              # setup, teardown, port-forward, data loading
```

## Benchmarks

Three query scenarios are compared — same Silver Iceberg data, different engines and caching strategies:

| Scenario | Engine | What it measures |
|----------|--------|-----------------|
| `trino_scan` | Trino 435 | Parquet scan via REST catalog + MinIO, file-system cache |
| `starrocks_scan` | StarRocks 3.4 CN | Same files via `iceberg_catalog`, CN block cache |
| `starrocks_mv` | StarRocks 3.4 CN | Pre-materialized Gold MVs — no Iceberg scan |

Pipeline metrics are also collected: Bronze ingestion throughput (rows/s from PostgreSQL WAL commit to Iceberg write), E2E latency percentiles (P50/P95/P99), and Silver MERGE throughput.

**Protocol:** ClickBench-inspired — cold cache (Trino worker restart + StarRocks `DROP ALL CACHE`) followed by 2 warmup + 3 timed runs. Geometric mean reported.

```bash
python -m benchmarks.pipeline_benchmark   # Bronze ingestion + E2E latency + Silver MERGE
python -m benchmarks.engine_compare       # cold + warm query comparison across 3 scenarios
python -m benchmarks.report               # generate showcase Markdown report
```

See [docs/benchmarks/methodology.md](docs/benchmarks/methodology.md) for full methodology, query descriptions, and reproducibility guide.

## Prerequisites Installation

```bash
# macOS
brew install k3d kubectl helm

# Verify
k3d version    # >= 5.6
kubectl version --client
helm version   # >= 3.14
```
