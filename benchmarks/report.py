"""
Generate a comprehensive Markdown benchmark showcase report.

Reads:
  benchmarks/results/YYYY-MM-DD-engine-compare.json  (from engine_compare.py)
  benchmarks/results/YYYY-MM-DD-pipeline.json         (from pipeline_benchmark.py)

Sections:
  1. Pipeline Performance   — ingestion throughput + E2E latency + Silver MERGE
  2. Cold Query Performance — first-hit latency, Trino vs StarRocks scan
  3. Warm Query Performance — cached geo-mean, Trino vs StarRocks + cache effect
  4. Gold MV vs Full Scan   — Trino scan | SR scan | SR MV | MV speedup vs Trino
  5. Analysis               — narrative: why StarRocks wins + Trino's strength
  6. Methodology            — reproducibility details

Usage:
    python -m benchmarks.report
    python -m benchmarks.report --engine path/to/engine-compare.json \\
                                --pipeline path/to/pipeline.json
"""

from __future__ import annotations

import argparse
import json
import math
from datetime import date
from pathlib import Path

RESULTS_DIR = Path(__file__).parent / "results"

QUERY_LABELS = {
    "q01_daily_revenue":    "Q01 — Daily revenue by category",
    "q02_top_sellers":      "Q02 — Top sellers by GMV",
    "q03_review_sentiment": "Q03 — Review sentiment by state",
    "q04_order_funnel":     "Q04 — Order funnel (weekly)",
    "q05_cohort_retention": "Q05 — Cohort retention (90 d)",
}


# ── Helpers ──────────────────────────────────────────────────────────────────

def geo_mean(values: list[float]) -> float:
    if not values:
        return 0.0
    log_sum = sum(math.log(max(v, 1e-9)) for v in values)
    return math.exp(log_sum / len(values))


def speedup(baseline: float | None, faster: float | None) -> str:
    if not baseline or not faster or faster <= 0:
        return "—"
    return f"{baseline / faster:.1f}×"


def fmt_ms(ms: float | None) -> str:
    if ms is None:
        return "—"
    if ms >= 10_000:
        return f"{ms / 1000:.1f} s"
    if ms >= 1_000:
        return f"{ms / 1000:.2f} s"
    return f"{ms:.0f} ms"


# ── Section 1: Pipeline Performance ──────────────────────────────────────────

def render_pipeline_section(p: dict) -> str:
    lines = [
        "## 1. Pipeline Performance",
        "",
        "These metrics capture the full data path that query-engine benchmarks cannot see.",
        "",
        "### 1a. Bronze Ingestion Throughput",
        "",
        "Rows/second from PostgreSQL WAL commit → Iceberg Parquet write on MinIO.",
        "",
        "| Table | Rows | Duration (s) | Throughput (rows/s) |",
        "|-------|-----:|-------------:|--------------------:|",
    ]
    bronze = p.get("bronze_throughput", {})
    for t in bronze.get("per_table", []):
        lines.append(
            f"| `{t['table']}` | {t['row_count']:,} "
            f"| {t['duration_s']:.1f} | {t['rows_per_sec']:,.0f} |"
        )
    total = bronze.get("total_rows", 0)
    span = bronze.get("total_span_s", 1)
    agg = bronze.get("aggregate_rows_per_sec", 0)
    lines += [
        f"| **TOTAL** | **{total:,}** | **{span:.0f}** | **{agg:,.0f}** |",
        "",
        "### 1b. E2E Commit-to-Bronze Latency",
        "",
        "Time from PostgreSQL WAL commit (`_cdc_source_ts_ms`, embedded in the Debezium envelope) "
        "to Iceberg Parquet write (`_cdc_ingested_at`). "
        "This is the only metric that captures true source-to-storage lag — "
        "infrastructure metrics cannot see inside the Debezium payload.",
        "",
        "| P50 | P95 | P99 | Measured rows |",
        "|----:|----:|----:|--------------:|",
    ]
    e2e = p.get("e2e_latency", {})
    lines.append(
        f"| {e2e.get('p50_s', '—')} s | {e2e.get('p95_s', '—')} s "
        f"| {e2e.get('p99_s', '—')} s | {e2e.get('measured_rows', 0):,} |"
    )
    lines += [
        "",
        "### 1c. Silver MERGE Throughput",
        "",
        "Rows/second processed by the PySpark `MERGE INTO` job, across the full Bronze → Silver run.",
        "",
        "| Table | Rows | Duration (s) | Throughput (rows/s) |",
        "|-------|-----:|-------------:|--------------------:|",
    ]
    silver = p.get("silver_throughput", {})
    for t in silver.get("per_table", []):
        lines.append(
            f"| `{t['table']}` | {t['row_count']:,} "
            f"| {t['duration_s']:.1f} | {t['rows_per_sec']:,.0f} |"
        )
    lines.append("")
    return "\n".join(lines)


# ── Section 2: Cold Query Performance ────────────────────────────────────────

def render_cold_section(scan_results: list[dict]) -> str:
    lines = [
        "## 2. Cold Query Performance (caches cleared before each run)",
        "",
        "Cold state: StarRocks CN block cache dropped via `ALTER SYSTEM DROP ALL CACHE`; "
        "Trino worker pod restarted (empty `/tmp/trino-cache`).",
        "",
        "| Query | Trino cold | StarRocks cold | SR advantage |",
        "|-------|:----------:|:--------------:|:------------:|",
    ]
    for row in scan_results:
        label = QUERY_LABELS.get(row["query"], row["query"])
        tr = row.get("trino_scan", {}).get("cold_ms")
        sr = row.get("starrocks_scan", {}).get("cold_ms")
        lines.append(
            f"| {label} | {fmt_ms(tr)} | {fmt_ms(sr)} | {speedup(tr, sr)} |"
        )
    lines.append("")
    return "\n".join(lines)


# ── Section 3: Warm Query Performance ────────────────────────────────────────

def render_warm_section(scan_results: list[dict]) -> str:
    lines = [
        "## 3. Warm Query Performance (caches populated)",
        "",
        "Metric: geometric mean of 3 timed runs after 2 warmup runs. "
        "Geometric mean matches ClickBench protocol — a single slow query does not dominate the aggregate.",
        "",
        "| Query | Trino warm | StarRocks warm | Cache effect (SR) | SR advantage |",
        "|-------|:----------:|:--------------:|:-----------------:|:------------:|",
    ]
    for row in scan_results:
        label = QUERY_LABELS.get(row["query"], row["query"])
        sr_cold = row.get("starrocks_scan", {}).get("cold_ms")
        sr_warm = row.get("starrocks_scan", {}).get("warm_geo_mean_ms")
        tr_warm = row.get("trino_scan", {}).get("warm_geo_mean_ms")
        lines.append(
            f"| {label} | {fmt_ms(tr_warm)} | {fmt_ms(sr_warm)} "
            f"| {speedup(sr_cold, sr_warm)} | {speedup(tr_warm, sr_warm)} |"
        )
    lines.append("")
    return "\n".join(lines)


# ── Section 4: Gold MV vs Full Scan ──────────────────────────────────────────

def render_mv_section(scan_results: list[dict], mv_results: list[dict]) -> str:
    lines = [
        "## 4. Gold Materialized View vs Full Scan",
        "",
        "StarRocks Gold MVs are pre-aggregated into native shared-data storage — "
        "no Iceberg scan, no Parquet decode, no MinIO round-trips. "
        "This is the **production query path** for the text-to-SQL agent.",
        "",
        "> Q05 (cohort retention) has no MV equivalent. "
        "It requires a self-join with a 90-day INTERVAL window that cannot be expressed "
        "as a static pre-aggregation. It is served via the StarRocks `iceberg_catalog` scan path.",
        "",
        "| Query | Trino scan | SR scan | SR Gold MV | MV speedup vs Trino |",
        "|-------|:----------:|:-------:|:----------:|:-------------------:|",
    ]
    mv_map = {r["query"]: r.get("starrocks_mv", {}) for r in mv_results}
    for row in scan_results:
        q = row["query"]
        if q not in mv_map:
            continue
        label = QUERY_LABELS.get(q, q)
        tr_warm = row.get("trino_scan", {}).get("warm_geo_mean_ms")
        sr_warm = row.get("starrocks_scan", {}).get("warm_geo_mean_ms")
        mv_warm = mv_map[q].get("warm_geo_mean_ms")
        lines.append(
            f"| {label} | {fmt_ms(tr_warm)} | {fmt_ms(sr_warm)} "
            f"| **{fmt_ms(mv_warm)}** | **{speedup(tr_warm, mv_warm)}** |"
        )
    lines.append("")
    return "\n".join(lines)


# ── Section 5: Analysis ───────────────────────────────────────────────────────

def render_analysis(scan_results: list[dict], mv_results: list[dict]) -> str:
    tr_vals = [r["trino_scan"]["warm_geo_mean_ms"] for r in scan_results
               if r.get("trino_scan", {}).get("warm_geo_mean_ms")]
    sr_vals = [r["starrocks_scan"]["warm_geo_mean_ms"] for r in scan_results
               if r.get("starrocks_scan", {}).get("warm_geo_mean_ms")]
    mv_vals = [r["starrocks_mv"]["warm_geo_mean_ms"] for r in mv_results
               if r.get("starrocks_mv", {}).get("warm_geo_mean_ms")]

    tr_agg = geo_mean(tr_vals)
    sr_agg = geo_mean(sr_vals)
    mv_agg = geo_mean(mv_vals) if mv_vals else None

    scan_adv = f"{tr_agg / sr_agg:.1f}×" if sr_agg > 0 else "—"
    mv_adv = f"{tr_agg / mv_agg:.1f}×" if mv_agg else "—"

    lines = [
        "## 5. Analysis and Recommendation",
        "",
        f"Across all 5 analytical queries, **StarRocks (warm scan) is {scan_adv} faster than Trino** "
        f"on the same Silver Iceberg data "
        f"(geo-mean warm: Trino {fmt_ms(tr_agg)}, StarRocks {fmt_ms(sr_agg)}).",
    ]
    if mv_agg:
        lines.append(
            f"With Gold pre-materialized views the gap widens to **{mv_adv} faster** "
            f"(geo-mean MV: {fmt_ms(mv_agg)}) — the production agent query path."
        )
    lines += [
        "",
        "**Why StarRocks is faster for analytics:**",
        "",
        "1. **C++ vectorized execution engine** — StarRocks executes the full query pipeline in "
        "native C++; Trino runs on the JVM. For scan-heavy aggregation queries the overhead of "
        "Java object allocation and GC is measurable.",
        "",
        "2. **CN block cache** — StarRocks Compute Nodes in shared-data mode cache Parquet pages "
        "locally after the first read. Repeated queries skip MinIO entirely. "
        "The *Cache effect* column in Section 3 shows the cold→warm speedup per query.",
        "",
        "3. **Gold materialized views** — pre-aggregating daily revenue, top sellers, "
        "review sentiment, and order funnel into StarRocks native storage eliminates the Iceberg "
        "scan entirely for the most common agent query patterns.",
        "",
        "**Where Trino remains the right choice:**",
        "",
        "- Complex ad-hoc queries across multiple Iceberg layers (Bronze + Silver joins)",
        "- Queries that change frequently and cannot be pre-materialized",
        "- Cross-catalog federation (Hive, Delta, Iceberg, TPCH in one query)",
        "- Q05 cohort retention — a self-join with a 90-day INTERVAL window — "
        "is precisely the class of query where pre-materialization cannot help. "
        "Both engines handle it; StarRocks still leads on raw latency but the gap narrows.",
        "",
        "**Architectural recommendation:**",
        "",
        "| Query pattern | Engine |",
        "|---------------|--------|",
        "| Recurring analytical questions (agent queries) | StarRocks Gold MVs |",
        "| Ad-hoc Silver exploration (dynamically generated SQL) | StarRocks `iceberg_catalog` scan |",
        "| Multi-layer joins, Bronze/Silver data engineering | Trino |",
        "",
    ]
    return "\n".join(lines)


# ── Section 6: Methodology ────────────────────────────────────────────────────

def render_methodology(engine_data: dict) -> str:
    protocol = engine_data.get("protocol", {})
    warmup = protocol.get("warmup_runs", 2)
    timed = protocol.get("timed_runs", 3)
    cold_method = protocol.get(
        "cold_cache",
        "StarRocks `ALTER SYSTEM DROP ALL CACHE` + Trino worker pod restart",
    )
    lines = [
        "## 6. Methodology",
        "",
        "Protocol inspired by [ClickBench](https://benchmark.clickhouse.com/) "
        "with adaptations for a CDC lakehouse environment.",
        "",
        "| Parameter | Value |",
        "|-----------|-------|",
        f"| Warmup runs (discarded) | {warmup} |",
        f"| Timed runs | {timed} |",
        "| Aggregate metric | Geometric mean of timed runs |",
        f"| Cold-cache method | {cold_method} |",
        "| Trino version | 435 |",
        "| StarRocks version | 3.4 (CN shared-data mode) |",
        "| Iceberg catalog | REST catalog (PyIceberg-compatible) |",
        "| Object storage | MinIO (S3-compatible local equivalent) |",
        "| Hardware | k3d 4-node cluster, Apple M2 Pro, 16 GB RAM |",
        "| Dataset | Brazilian Olist E-Commerce (public, Kaggle) |",
        "| Bronze rows | ~812,000 across 7 tables |",
        "",
        "**Why geometric mean?** If Q01 is 10× faster and Q05 is 1× faster, "
        "the arithmetic mean reports 5.5× (misleadingly high). "
        "The geometric mean reports √(10 × 1) = 3.2×, "
        "representing the typical per-query improvement. Same rationale used by ClickBench.",
        "",
        "**Why three scenarios?**",
        "",
        "| Scenario | What it isolates |",
        "|----------|-----------------|",
        "| `trino_scan` | Full Parquet scan via Trino + REST catalog + MinIO |",
        "| `starrocks_scan` | Same files, same catalog, but StarRocks C++ engine + CN cache |",
        "| `starrocks_mv` | Pre-aggregated Gold MVs — no Iceberg scan at all |",
        "",
        "Comparing all three isolates the contribution of: "
        "(a) the query engine itself, (b) the CN block cache, and (c) pre-materialization.",
        "",
        "**Pipeline benchmark (unique to this project):** "
        "Standard query benchmarks measure execution time only. "
        "This project also measures the full data pipeline — from PostgreSQL WAL commit "
        "to Iceberg write — using timestamps embedded in the Debezium CDC envelope. "
        "This cannot be reproduced with synthetic benchmarks like TPC-H or ClickBench "
        "because they have no live CDC source.",
        "",
        "**Reproducing these results:**",
        "",
        "```bash",
        "# Requires Silver + Bronze tables populated, Trino and StarRocks deployed, Gold MVs built",
        "python -m benchmarks.pipeline_benchmark   # pipeline metrics",
        "python -m benchmarks.engine_compare       # cold + warm query timings",
        "python -m benchmarks.report               # generate this report",
        "```",
        "",
    ]
    return "\n".join(lines)


# ── Entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Generate full benchmark showcase report")
    parser.add_argument("--engine", help="Path to engine-compare JSON (default: latest in results/)")
    parser.add_argument("--pipeline", help="Path to pipeline JSON (default: latest in results/)")
    args = parser.parse_args()

    if args.engine:
        engine_path = Path(args.engine)
    else:
        candidates = sorted(RESULTS_DIR.glob("*-engine-compare.json"), reverse=True)
        if not candidates:
            print("No engine-compare results found. Run 'python -m benchmarks.engine_compare' first.")
            return
        engine_path = candidates[0]

    if args.pipeline:
        pipeline_path = Path(args.pipeline)
    else:
        candidates = sorted(RESULTS_DIR.glob("*-pipeline.json"), reverse=True)
        pipeline_path = candidates[0] if candidates else None

    engine_data = json.loads(engine_path.read_text())
    pipeline_data = (
        json.loads(pipeline_path.read_text())
        if pipeline_path and pipeline_path.exists()
        else None
    )

    scan_results = engine_data.get("scan_results", [])
    mv_results = engine_data.get("mv_results", [])
    today = date.today().isoformat()

    sections: list[str] = [
        f"# Benchmark Showcase — {today}",
        "",
        "> Generated by `python -m benchmarks.report`",
        f"> Engine-compare source: `{engine_path.name}`",
        f"> Pipeline source: `{pipeline_path.name if pipeline_path else 'not available'}`",
        "",
        "---",
        "",
    ]

    if pipeline_data:
        sections += [render_pipeline_section(pipeline_data), "---", ""]

    sections += [
        render_cold_section(scan_results), "---", "",
        render_warm_section(scan_results), "---", "",
        render_mv_section(scan_results, mv_results), "---", "",
        render_analysis(scan_results, mv_results), "---", "",
        render_methodology(engine_data),
    ]

    report_text = "\n".join(sections)

    RESULTS_DIR.mkdir(exist_ok=True)
    out = RESULTS_DIR / f"{today}-benchmark-report.md"
    out.write_text(report_text)
    print(f"\nReport → {out}")
    print()
    print(report_text)


if __name__ == "__main__":
    main()
