# ADR-006: OpenTelemetry as the Observability Collection Layer

**Status:** Accepted  
**Date:** 2026-05

## Context

The lakehouse has 9 components across 3 runtimes (JVM, Python, native Go/C). The original plan was Prometheus scrape + Grafana. Two collection strategies were considered:

1. **Direct Prometheus scrape** — each component exposes `/metrics` in Prometheus text format; Prometheus scrapes on a pull cycle
2. **OpenTelemetry Collector as hub** — each component pushes OTLP to a central collector which fans out to backends

## Decision

Use the **OpenTelemetry Collector** as the single ingestion point for all telemetry. Backends: Prometheus for metrics, Grafana Tempo for traces. Grafana is the single UI for both.

**Why OTel Collector over direct Prometheus scrape:**

Direct scrape works for native Go components (MinIO, Grafana itself) but fails for JVM components. Kafka, Spark, and Trino expose metrics via JMX — not HTTP/Prometheus format. Bridging JMX to Prometheus requires a `jmx_exporter` Java agent *plus* a `/metrics` HTTP server per component. The OTel Java agent already provides JVM metrics via OTLP push with zero extra infrastructure.

The push model also enables the project's headline metric — `pipeline.bronze.e2e_latency_seconds` — which correlates two timestamps from different systems (PostgreSQL CDC commit time from the Debezium event payload, and Spark ingestion time). This cannot be derived from scrape alone; it requires application-level instrumentation inside the Spark Bronze job.

**Why Grafana Tempo over Jaeger for traces:**

Jaeger requires Cassandra or Elasticsearch as its storage backend — two services that don't already exist in the stack. Tempo stores trace blocks as files in object storage (S3-compatible). MinIO is already deployed with spare capacity; adding a `lakehouse-traces` bucket is the only infrastructure change. Tempo also integrates more tightly with Grafana (native datasource, `tracesToMetrics` linking, node graph).

**Trace strategy — drill-down, not default-on:**

Traces are emitted at 10% sample rate for high-throughput components (Kafka, Spark). 100% for the FastAPI agent (low volume, every user query is portfolio-worthy). `parentbased_traceidratio` ensures all spans within a single trace are either fully sampled or fully dropped — no orphaned child spans.

**Deployment strategy — instrument as you build:**

The OTel Collector and Prometheus are deployed alongside Debezium so all subsequent components have a live OTLP endpoint from day one. Grafana Tempo and Grafana UI are deployed once the full pipeline exists and there is data to visualise.

## Consequences

**Good:**
- Single OTLP endpoint for all components — one env var (`OTEL_EXPORTER_OTLP_ENDPOINT`) is all each component needs
- Backends are decoupled from instrumentation — switch Prometheus to VictoriaMetrics with one Collector config change
- Tempo reuses MinIO — no new database
- `pipeline.bronze.e2e_latency_seconds` is impossible without application-level SDK instrumentation; this forces meaningful observability rather than infrastructure-only metrics

**Bad:**
- OTel Collector is a new moving part that must be deployed before any instrumented component starts
- The debug exporter in the Collector config logs all received telemetry — should be removed before production use

## Instrumentation per runtime

| Runtime | Method | Version |
|---------|--------|---------|
| JVM (Kafka, Debezium, Iceberg, Spark, Trino) | `JAVA_TOOL_OPTIONS=-javaagent:/otel/opentelemetry-javaagent.jar` via init container | 2.4.0 |
| Python (Airflow, FastAPI) | `opentelemetry-instrument <app>` or in-process SDK | opentelemetry-distro 0.46b0 |
| MinIO | Prometheus scrape receiver in OTel Collector (`/minio/v2/metrics/cluster`) | native |
| PostgreSQL | `postgres_exporter` Deployment → Prometheus scrape receiver | v0.15.0 |
