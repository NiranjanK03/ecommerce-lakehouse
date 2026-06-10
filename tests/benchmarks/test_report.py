import json
import pytest
from benchmarks.report import render_markdown, write_report, speedup

SAMPLE_RESULTS = [
    {
        "query": "q01_daily_revenue",
        "trino":     {"p50": 1800.0, "p95": 2300.0, "runs": []},
        "starrocks": {"p50": 500.0,  "p95": 800.0,  "runs": []},
    },
    {
        "query": "q05_cohort_retention",
        "trino":     {"p50": 5800.0, "p95": 7200.0, "runs": []},
        "starrocks": {"p50": 1600.0, "p95": 2100.0, "runs": []},
    },
]

META = {
    "date": "2026-05-25",
    "runs": 10,
    "trino_version": "435",
    "starrocks_version": "3.2.6",
    "hardware": "k3d 4-node",
    "dataset": "Olist 1×",
}


def test_speedup_calculation():
    assert speedup(2300.0, 800.0) == 2.9


def test_speedup_zero_denominator_returns_zero():
    assert speedup(2300.0, 0.0) == 0.0


def test_render_markdown_contains_header():
    md = render_markdown(SAMPLE_RESULTS, META)
    assert "## Engine Comparison" in md
    assert "2026-05-25" in md


def test_render_markdown_contains_query_row():
    md = render_markdown(SAMPLE_RESULTS, META)
    assert "Q01 daily revenue by category" in md
    assert "Q05 cohort retention" in md


def test_render_markdown_contains_speedup():
    md = render_markdown(SAMPLE_RESULTS, META)
    assert "2.9×" in md


def test_write_report_creates_md_and_json(tmp_path):
    md_path, json_path = write_report(SAMPLE_RESULTS, META, tmp_path)
    assert md_path.exists()
    assert json_path.exists()


def test_write_report_json_is_parseable(tmp_path):
    _, json_path = write_report(SAMPLE_RESULTS, META, tmp_path)
    data = json.loads(json_path.read_text())
    assert data["meta"]["date"] == "2026-05-25"
    assert len(data["results"]) == 2


def test_write_report_creates_output_dir(tmp_path):
    nested = tmp_path / "deep" / "nested"
    write_report(SAMPLE_RESULTS, META, nested)
    assert nested.exists()
