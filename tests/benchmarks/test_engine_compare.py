import pytest
from benchmarks.engine_compare import time_query, compare_engines


class MockEngine:
    def __init__(self, engine_name: str):
        self._name = engine_name
        self.call_count = 0

    def name(self) -> str:
        return self._name

    def execute(self, sql: str) -> list:
        self.call_count += 1
        return [{"result": 1}]


def test_time_query_includes_one_warmup_run():
    engine = MockEngine("test")
    time_query(engine, "SELECT 1", runs=5)
    # 1 warmup + 5 timed = 6 total calls
    assert engine.call_count == 6


def test_time_query_returns_p50_and_p95():
    engine = MockEngine("test")
    result = time_query(engine, "SELECT 1", runs=10)
    assert "p50" in result
    assert "p95" in result
    assert len(result["runs"]) == 10


def test_time_query_p95_gte_p50():
    engine = MockEngine("test")
    result = time_query(engine, "SELECT 1", runs=10)
    assert result["p95"] >= result["p50"]


def test_compare_engines_covers_all_query_files(tmp_path):
    (tmp_path / "q01_test.sql").write_text("SELECT 1")
    (tmp_path / "q02_test.sql").write_text("SELECT 2")
    engines = [MockEngine("trino"), MockEngine("starrocks")]
    results = compare_engines(engines, queries_dir=tmp_path, runs=2)
    assert len(results) == 2
    assert results[0]["query"] == "q01_test"
    assert "trino" in results[0]
    assert "starrocks" in results[0]


def test_compare_engines_empty_dir_returns_empty(tmp_path):
    engines = [MockEngine("trino")]
    results = compare_engines(engines, queries_dir=tmp_path, runs=2)
    assert results == []
