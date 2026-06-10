# ADR-005: Physical Iceberg Tables at Every Medallion Layer

**Status:** Accepted  
**Date:** 2026-05-15

## Context

Bronze → Silver → Gold superficially looks like three copies of the same data. The question was raised: should Silver and Gold be Trino materialized views on top of Bronze rather than separate physical Iceberg tables, to avoid storage amplification?

## Decision

Use physical Iceberg tables at all three layers. Use dbt `incremental` models (Trino `MERGE INTO`) for Silver to avoid full re-processing. Evaluate Trino logical views (not materialized) for simple Gold aggregations on a per-model basis.

## Reasoning

**The layers are not copies — they are different data models.**  
Bronze contains raw Debezium CDC events (one row per database change event, with `op`, `before`, `after`, `source` fields). Silver contains current entity state (one row per primary key, clean types, CDC mechanics stripped out). The `MERGE INTO` that collapses N update events into one Silver row cannot be expressed as a view — it requires imperative write logic.

**Trino materialized view refresh is always full.**  
Trino MVs rewrite the entire table on every refresh. dbt incremental models process only new Bronze rows since the last run. For 100k orders with daily CDC activity, incremental processes hundreds of changed rows; a full MV refresh processes the entire table. Incremental wins on both latency and cost.

**Trino MVs are backed by Iceberg tables anyway.**  
There is no storage saving — Trino stores the MV as an Iceberg table under the hood. The only difference is losing control over the write path and partitioning strategy.

**Layer decoupling and time travel.**  
Physical Silver and Gold tables maintain their own Iceberg snapshot history independently of Bronze. A bad Bronze CDC batch does not corrupt a Silver table that was last written before the bad data arrived. Querying `silver.orders FOR SYSTEM_TIME AS OF '...'` to debug a Gold anomaly requires Silver to be a physical table with its own snapshots.

## Storage amplification in practice

Silver is smaller than Bronze: 10 UPDATE events on `order_id = X` → 1 Silver row. Gold is smaller than Silver: row-level order data aggregated to daily/category summaries. Parquet + Snappy compression compounds this. The actual storage overhead across three layers is moderate and justified by the benefits above.

## Exception

Trino logical views (not materialized, zero storage) are appropriate for Gold models that are simple `GROUP BY` aggregations over a well-partitioned Silver table, where query latency is acceptable. This is evaluated per model, not as a blanket rule.

## Alternatives Rejected

**Materialized views for Silver/Gold:** Full-refresh semantics, same storage cost, loss of per-layer time travel, no layer decoupling. Rejected.

**Single physical table with a `layer` column:** Collapses Bronze/Silver/Gold into one table. Destroys the immutability guarantee of Bronze and the schema clarity of Silver/Gold. Rejected.
