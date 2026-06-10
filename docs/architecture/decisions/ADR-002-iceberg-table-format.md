# ADR-002: Apache Iceberg as the Table Format

**Status:** Accepted  
**Date:** 2024-01

## Context

The lakehouse needs a table format that sits on top of raw Parquet files in object storage and provides ACID transactions, schema evolution, and time-travel. The main candidates are Apache Iceberg, Apache Hudi, and Delta Lake.

## Decision

Use Apache Iceberg.

**Why Iceberg over Delta Lake:**  
Delta Lake was created by Databricks and its governance is still heavily Databricks-influenced despite the Linux Foundation move. Delta format version 3 features (Deletion Vectors, Liquid Clustering) ship in the Databricks runtime before the OSS version. This project commits to components that work identically in open-source and managed environments.

**Why Iceberg over Hudi:**  
Hudi has stronger write-side semantics (record-level upserts, Merge-on-Read) but its catalog integration and engine support are less mature. Iceberg has native first-class support in Spark 3.x, Trino, PyIceberg, and Flink. Adding a new engine (e.g. DuckDB) requires zero format changes.

**Key Iceberg features used in this project:**
- **Append-only writes for Bronze:** Iceberg's snapshot model makes Bronze tables naturally immutable after each write.
- **Merge-on-Write for Silver:** `MERGE INTO` semantics handle late-arriving CDC events.
- **Time travel:** `SELECT * FROM silver.orders FOR SYSTEM_TIME AS OF '2024-01-15'` for debugging.
- **Schema evolution:** Adding columns to Silver without rewriting files.
- **Partition evolution:** Change partition strategy without rewriting existing data.

## Consequences

**Good:** Engine-agnostic. Excellent catalog ecosystem (REST, Hive, Nessie). Strong community (Apache top-level project).

**Bad:** Iceberg's REST Catalog is a relatively recent spec — some older tooling still assumes Hive Metastore. Not a concern in this stack since all chosen tools support REST natively.
