"""
Query engine benchmark: Trino vs StarRocks (scan) vs StarRocks (Gold MV).

Three scenarios
---------------
trino_scan      Trino 435 queries Silver Iceberg tables via native S3 + REST catalog.
                File-system cache enabled — first run is cold, subsequent runs are warm.

starrocks_scan  StarRocks 3.4 CN pods query Silver via the external iceberg_catalog.
                Data cache (block cache) is enabled in CN shared-data mode.

starrocks_mv    StarRocks queries pre-materialised Gold MVs — no Iceberg scan at all.
                Represents the production text-to-SQL agent query path.

Protocol (ClickBench-inspired)
-------------------------------
Cold phase  : caches are cleared (StarRocks DROP ALL CACHE; Trino worker restart).
              Each query runs once. cold_ms = wall time of that single run.

Warm phase  : 2 warmup runs (not recorded) let caches fill, then 3 timed runs.
              warm_geo_mean_ms = geometric mean of the 3 timed runs.
              Geometric mean chosen (vs arithmetic) so a single slow run on one
              query does not dominate the aggregate, consistent with ClickBench.

Outputs
-------
benchmarks/results/YYYY-MM-DD-engine-compare.json  (raw timings)
Pass to `python -m benchmarks.report` to generate the Markdown showcase.

Usage
-----
    python -m benchmarks.engine_compare          # full cold+warm run
    python -m benchmarks.engine_compare --warm-only  # skip cache-clear phase
"""

from __future__ import annotations

import argparse
import json
import math
import subprocess
import time
from datetime import date
from pathlib import Path
from typing import Optional

import mysql.connector
import trino

from benchmarks.config import get_starrocks_conn, get_trino_conn, load_config

QUERIES_DIR = Path(__file__).parent / "queries"
MV_QUERIES_DIR = QUERIES_DIR / "mv"
RESULTS_DIR = Path(__file__).parent / "results"
WARMUP_RUNS = 2
TIMED_RUNS = 3


# ── Geometric mean ──────────────────────────────────────────────────────────────

def geo_mean(values: list[float]) -> float:
    """Geometric mean — same aggregation used by ClickBench."""
    if not values:
        return 0.0
    log_sum = sum(math.log(max(v, 1e-9)) for v in values)
    return round(math.exp(log_sum / len(values)), 1)


# ── Engine classes ──────────────────────────────────────────────────────────────

class TrinoScanEngine:
    """
    Trino queries Silver Iceberg via native S3 + REST catalog.
    The Trino connection sets catalog=lakehouse, schema=silver so queries
    reference tables as plain `silver.table` without catalog prefix.
    """

    name = "trino_scan"
    label = "Trino 435 (Silver scan)"

    def __init__(self, conn_params: dict) -> None:
        self._p = conn_params

    def execute(self, sql: str) -> list:
        conn = trino.dbapi.connect(
            host=self._p["host"],
            port=self._p["port"],
            user="trino",
            catalog=self._p["catalog"],
            schema=self._p["schema"],
        )
        try:
            cur = conn.cursor()
            cur.execute(sql)
            return cur.fetchall()
        finally:
            conn.close()

    def clear_cache(self) -> None:
        """
        Trino has no SQL command to evict its file-system cache.
        Restart the worker pod — the new pod starts with an empty /tmp/trino-cache.
        The benchmark waits for the rollout to complete before running cold queries.
        """
        print("    [cache clear] restarting Trino worker pod...", end="", flush=True)
        subprocess.run(
            ["kubectl", "rollout", "restart", "deployment/trino-worker", "-n", "serving"],
            check=True, capture_output=True,
        )
        subprocess.run(
            ["kubectl", "rollout", "status", "deployment/trino-worker", "-n", "serving",
             "--timeout=180s"],
            check=True, capture_output=True,
        )
        time.sleep(10)  # let coordinator detect the new worker
        print(" done")


class StarRocksScanEngine:
    """
    StarRocks CN pods query Silver Iceberg via the external iceberg_catalog.
    SET CATALOG is issued once per connection — queries use plain `silver.table`.
    CN block cache is enabled in shared-data mode; cold state achieved via
    ALTER SYSTEM DROP ALL CACHE.
    """

    name = "starrocks_scan"
    label = "StarRocks 3.4 (Silver scan, CN cache)"

    def __init__(self, conn_params: dict) -> None:
        self._p = conn_params

    def _connect(self):
        return mysql.connector.connect(**self._p)

    def execute(self, sql: str) -> list:
        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute("SET CATALOG iceberg_catalog")
            cur.execute(sql)
            return cur.fetchall()
        finally:
            conn.close()

    def clear_cache(self) -> None:
        """Drop all data cached in CN block cache."""
        print("    [cache clear] StarRocks DROP ALL CACHE...", end="", flush=True)
        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute("ALTER SYSTEM DROP ALL CACHE")
        except Exception as exc:
            print(f" (warning: {exc})", end="")
        finally:
            conn.close()
        time.sleep(3)
        print(" done")


class StarRocksMVEngine:
    """
    StarRocks queries pre-materialised Gold MVs.
    No Iceberg scan — data lives in StarRocks shared-data native storage.
    Represents the production path: text-to-SQL agent queries the Gold layer.
    Cache is always warm here (MVs are StarRocks-native, not Parquet on MinIO).
    """

    name = "starrocks_mv"
    label = "StarRocks 3.4 (Gold MV)"

    def __init__(self, conn_params: dict) -> None:
        # Override database to 'gold' for MV queries
        self._p = {**conn_params, "database": "gold"}

    def _connect(self):
        return mysql.connector.connect(**self._p)

    def execute(self, sql: str) -> list:
        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute(sql)
            return cur.fetchall()
        finally:
            conn.close()

    def clear_cache(self) -> None:
        pass  # MVs live in StarRocks native storage — no Parquet cache to clear


# ── Timing helpers ──────────────────────────────────────────────────────────────

def timed_run(engine, sql: str) -> float:
    """Run one query and return wall-clock ms."""
    t0 = time.perf_counter()
    engine.execute(sql)
    return round((time.perf_counter() - t0) * 1000, 1)


def benchmark_query(engine, sql: str) -> dict:
    """
    Cold run (1) + warmup runs (WARMUP_RUNS, not recorded) + timed runs (TIMED_RUNS).
    Returns cold_ms and warm_geo_mean_ms.
    """
    cold_ms = timed_run(engine, sql)

    for _ in range(WARMUP_RUNS):
        engine.execute(sql)

    warm_runs = [timed_run(engine, sql) for _ in range(TIMED_RUNS)]

    return {
        "cold_ms": cold_ms,
        "warm_geo_mean_ms": geo_mean(warm_runs),
        "warm_runs_ms": warm_runs,
    }


def benchmark_mv_query(engine, sql: str) -> dict:
    """
    MV queries are always warm (StarRocks native storage).
    Run WARMUP_RUNS + TIMED_RUNS, report geo-mean only.
    """
    for _ in range(WARMUP_RUNS):
        engine.execute(sql)

    warm_runs = [timed_run(engine, sql) for _ in range(TIMED_RUNS)]

    return {
        "warm_geo_mean_ms": geo_mean(warm_runs),
        "warm_runs_ms": warm_runs,
    }


# ── Main benchmark loop ─────────────────────────────────────────────────────────

def run_scan_benchmarks(
    engines: list,
    queries_dir: Path,
    warm_only: bool = False,
) -> list[dict]:
    """
    Run all scan-engine queries. Returns one result dict per query.
    """
    results = []
    query_files = sorted(queries_dir.glob("*.sql"))

    if not warm_only:
        print("\n── Cold phase (clearing caches) ──")
        for engine in engines:
            engine.clear_cache()

    for qfile in query_files:
        sql = qfile.read_text()
        row: dict = {"query": qfile.stem}

        for engine in engines:
            print(f"  {engine.label:40s}  {qfile.stem}", end="", flush=True)
            if warm_only:
                # warm-only: skip cold run, still do warmup + timed
                for _ in range(WARMUP_RUNS):
                    engine.execute(sql)
                warm_runs = [timed_run(engine, sql) for _ in range(TIMED_RUNS)]
                row[engine.name] = {
                    "cold_ms": None,
                    "warm_geo_mean_ms": geo_mean(warm_runs),
                    "warm_runs_ms": warm_runs,
                }
            else:
                metrics = benchmark_query(engine, sql)
                row[engine.name] = metrics
            gm = row[engine.name]["warm_geo_mean_ms"]
            cold = row[engine.name].get("cold_ms")
            cold_str = f"  cold={cold:.0f}ms" if cold else ""
            print(f"{cold_str}  warm={gm:.0f}ms")

        results.append(row)

    return results


def run_mv_benchmarks(engine: StarRocksMVEngine, mv_dir: Path) -> list[dict]:
    """Run MV queries — always warm, no cache clearing."""
    results = []
    print("\n── Gold MV phase ──")

    for qfile in sorted(mv_dir.glob("*.sql")):
        sql = qfile.read_text()
        print(f"  {engine.label:40s}  {qfile.stem}", end="", flush=True)
        metrics = benchmark_mv_query(engine, sql)
        results.append({"query": qfile.stem, engine.name: metrics})
        print(f"  warm={metrics['warm_geo_mean_ms']:.0f}ms")

    return results


# ── Entry point ─────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Trino vs StarRocks benchmark")
    parser.add_argument(
        "--warm-only", action="store_true",
        help="Skip cache-clear phase (faster; use when re-running warm scenario only)",
    )
    args = parser.parse_args()

    cfg = load_config()
    sr_conn = get_starrocks_conn(cfg)
    tr_conn = get_trino_conn(cfg)

    scan_engines = [
        TrinoScanEngine(tr_conn),
        StarRocksScanEngine(sr_conn),
    ]
    mv_engine = StarRocksMVEngine(sr_conn)

    print(f"Protocol: {WARMUP_RUNS} warmup + {TIMED_RUNS} timed runs | geo-mean reported")
    print(f"Queries dir: {QUERIES_DIR}")

    scan_results = run_scan_benchmarks(scan_engines, QUERIES_DIR, warm_only=args.warm_only)
    mv_results = run_mv_benchmarks(mv_engine, MV_QUERIES_DIR)

    output = {
        "date": date.today().isoformat(),
        "protocol": {
            "warmup_runs": WARMUP_RUNS,
            "timed_runs": TIMED_RUNS,
            "metric": "geometric_mean_ms",
            "cold_cache": "starrocks_DROP_ALL_CACHE + trino_worker_restart",
        },
        "scan_results": scan_results,
        "mv_results": mv_results,
    }

    RESULTS_DIR.mkdir(exist_ok=True)
    out = RESULTS_DIR / f"{date.today().isoformat()}-engine-compare.json"
    out.write_text(json.dumps(output, indent=2))
    print(f"\nResults → {out}")
    print("Run 'python -m benchmarks.report' to generate the Markdown report.")


if __name__ == "__main__":
    main()
