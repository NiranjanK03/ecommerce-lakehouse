# Ecommerce Lakehouse — Project Memory

## What this project is
A fully open-source, local-first data lakehouse built on Kubernetes.
Portfolio project for Niranjan Kulkarni — demonstrating Senior Data Engineer
capabilities across the full modern data stack with a quantified benchmark.

---

## Ground rules for Claude Code

### Always do this
- Explain every config file and every non-obvious decision in plain English
  before writing it — not after
- Write documentation alongside code, never as an afterthought
- Follow the installation order strictly (defined below) — components have
  hard dependencies
- When making an architectural choice, write a one-paragraph ADR in
  docs/architecture/decisions/ before implementing
- Use the namespace strategy defined below — never deploy to default namespace
- Update CONTEXT.md after completing each significant milestone

### Never do this
- Do not skip documentation to move faster
- Do not use Databricks, Delta Lake, or any vendor-managed service — this
  stack is fully open source
- Do not hardcode cloud credentials or endpoints — all environment-specific
  config goes in config/local.yaml
- Do not install components out of order

---

## Full tech stack

| Layer | Tool | Notes |
|---|---|---|
| Local Kubernetes | k3d | k3s in Docker, 1 server + 3 agent nodes |
| Object storage | MinIO | S3-compatible |
| Table format | Apache Iceberg | Bronze + Silver layers |
| Iceberg catalog | Iceberg REST Catalog | Tracks table metadata |
| Compute | Apache Spark (spark-on-k8s-operator) | PySpark jobs via SparkApplication CRDs |
| Gold query engine | StarRocks (shared-data) | Vectorised MPP, async materialized views |
| Benchmark comparison | Trino | Queries same Silver Iceberg data for comparison |
| Transformation | dbt-StarRocks | Async MVs for Gold layer |
| CDC | Debezium | Captures PostgreSQL changes |
| Streaming | Apache Kafka (Bitnami KRaft) | Event transport layer |
| Orchestration | Apache Airflow | KubernetesExecutor, event-driven Silver trigger |
| Observability | OTel Collector + Prometheus | Metrics and traces |
| Source database | PostgreSQL | Olist e-commerce dataset |
| Access | kubectl port-forward / k9s | No load balancer in local setup |

---

## Dataset
Brazilian Olist E-Commerce dataset (public, Kaggle).
Contains: orders, customers, products, sellers, reviews, payments.
~5,071,289 synthetic rows across 7 tables (generated from the Olist schema). Load into PostgreSQL; Debezium treats it as a live CDC source.

---

## Medallion architecture

```
Bronze  — Raw, append-only. Exact copy of source events from Kafka/CDC.
          Never modified after write.

Silver  — Cleaned, deduplicated, typed. PySpark MERGE INTO.
          One current-state row per primary key.

Gold    — Business-ready aggregates in StarRocks async materialized views.
          Pre-computed for sub-40ms query latency.
```

---

## Namespace strategy (Kubernetes)

| Namespace | What lives here |
|---|---|
| infrastructure | MinIO, Iceberg REST Catalog, PostgreSQL |
| streaming | Kafka, Debezium |
| processing | Spark operator, SparkApplication jobs |
| orchestration | Airflow |
| serving | StarRocks, Trino |
| observability | OTel Collector, Prometheus |

Never use the default namespace.

---

## Installation order (strict — components have hard dependencies)

1. k3d cluster
2. All namespaces
3. MinIO — everything writes here first
4. Iceberg REST Catalog — needs MinIO bucket to exist
5. PostgreSQL — CDC source
6. Kafka — must be up before Debezium
7. Debezium — connects to Postgres + Kafka
8. Spark operator — installs CRDs only, no jobs yet
9. Airflow — orchestrates Silver MERGE jobs
10. StarRocks — Gold query engine
11. Trino — benchmark comparison engine

---

## Port-forward map (local access)

| Service | Local port |
|---|---|
| MinIO Console | 9001 |
| MinIO API | 9000 |
| Airflow UI | 8080 |
| StarRocks MySQL | 9030 |
| StarRocks UI | 8086 |
| Trino UI | 8085 |
| Iceberg REST Catalog | 8181 |
| Kafka | 9092 |

All port-forwards managed via scripts/port-forward.sh

---

## Repo structure

```
ecommerce-lakehouse/
├── CLAUDE.md
├── CONTEXT.md
├── README.md
├── docs/
│   ├── architecture/decisions/   ← ADRs
│   ├── benchmarks/               ← methodology.md
│   ├── runbooks/                 ← one per component
│   └── interview-prep.md
├── infrastructure/k8s/
│   ├── cluster/k3d-config.yaml
│   └── helm/
│       ├── values/               ← minio.yaml, kafka.yaml etc.
│       └── charts/               ← iceberg-rest/, debezium/, kafka/
├── pipelines/
│   ├── airflow/dags/
│   ├── airflow/plugins/
│   ├── spark/jobs/
│   ├── spark/applications/       ← SparkApplication CRDs
│   └── debezium/connectors/
├── transform/dbt/
│   └── models/gold/              ← StarRocks async MVs
├── benchmarks/
│   ├── queries/                  ← scan + mv SQL
│   ├── results/                  ← JSON + Markdown reports
│   ├── engine_compare.py
│   ├── pipeline_benchmark.py
│   └── report.py
├── config/local.yaml             ← gitignored
├── data/olist-raw/               ← source CSVs
└── scripts/
```

---

## Documentation standard
Every component gets:
1. A runbook in docs/runbooks/ explaining how to deploy, access, and debug it
2. Inline comments in Helm values explaining non-obvious settings
3. An ADR for any significant architectural choice made during implementation

---

## Milestones (all complete)

- [x] Core infrastructure (k3d, MinIO, Iceberg catalog, PostgreSQL, Kafka)
- [x] CDC pipeline (Debezium → Kafka → Bronze Iceberg)
- [x] Silver layer (PySpark MERGE INTO, Airflow IcebergSnapshotSensor)
- [x] Gold layer (StarRocks shared-data, dbt-StarRocks async MVs)
- [x] Benchmarking (StarRocks vs Trino: 27× scan speedup, 197× MV speedup)
