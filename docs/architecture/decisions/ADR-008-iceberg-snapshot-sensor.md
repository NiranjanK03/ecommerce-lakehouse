# ADR-008: Event-Driven Silver Trigger via IcebergSnapshotSensor

**Status:** Accepted  
**Date:** 2026-05-25

## Context

The Silver layer runs a PySpark MERGE INTO job that reads new Bronze Iceberg snapshots and upserts into Silver. The question is: what triggers that job?

The original design used a fixed cron schedule (e.g., hourly). A time-based trigger introduces latency proportional to the interval — if Bronze writes at minute 1, Silver doesn't run until minute 60 of the next cycle. Reducing the interval to 5 minutes increases cluster load regardless of whether Bronze actually produced data.

## Decision

Use a custom Airflow sensor (`IcebergSnapshotSensor`) that polls the Iceberg REST catalog's snapshots endpoint every 30 seconds. When Bronze has a snapshot newer than the last recorded snapshot ID (stored in an Airflow Variable), the sensor fires and unblocks the Silver SparkApplication.

The pipeline is a self-retriggering loop: Silver completes → Gold MV refresh → `TriggerDagRunOperator` restarts the DAG → sensor waits for the next Bronze snapshot. This gives continuous near-real-time processing without a fixed schedule.

## Consequences

**Good:** Silver latency is bounded by one sensor poll interval (30 seconds) from the time Bronze writes, not a cron interval. No unnecessary Spark job launches when Bronze is idle. The self-retriggering loop is easier to reason about than a scheduled DAG with catchup logic.

**Bad:** The sensor adds one poll cycle (up to 30s) of latency that a direct trigger mechanism (Kafka trigger, webhook) would not. For this project's workload the extra 30 seconds is acceptable.

**Sensor resilience:** If Bronze is idle for more than `timeout=3600` seconds, the sensor task fails and Airflow alerts. This surfaces problems (Debezium down, Kafka backpressure) rather than silently waiting.

## Alternatives Considered

**Fixed cron schedule:** Simpler to configure but introduces minimum latency equal to the cron interval, and wastes compute on empty Bronze runs.

**Kafka-triggered Airflow:** Use Confluent's `AwaitMessageTriggerFunctionSensor` to trigger on new Kafka messages. More direct but adds a Confluent dependency and tighter coupling between the streaming and orchestration layers.
