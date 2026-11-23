"""Tests for scripts/measure_aws_resources.py's --by-run mode: grouping
warehouse.etl_process_log rows by full_run_id and computing the cross-run
mean/median/stdev statistics. Pure-function tests only -- no real Postgres connection,
mirroring the lakehouse repo's tests/test_measure_aws_resources.py (FakeAthena there,
plain dict rows here since this repo's per-table log lives in Postgres). dpu_seconds is
read directly from the row (cli.py's _run_glue_step backfills it once a Glue job run is
terminal -- see glue_common.log_timed_call's docstring), not re-derived via the Glue API,
so collect_runs here is a pure function with no AWS dependency at all."""

from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))
sys.path.insert(0, str(REPO_ROOT / "scripts"))

import measure_aws_resources as mar


def _row(full_run_id, run_id, job_name, process_type, process_name, phase, start, end, exec_s,
        dpu=None, raw=None, gold=None, status="SUCCEEDED", dataset="demodataset"):
    return {
        "full_run_id": full_run_id, "run_id": run_id, "job_name": job_name,
        "process_name": process_name, "process_type": process_type, "phase": phase,
        "dataset": dataset, "start_ts": datetime.fromisoformat(start), "end_ts": datetime.fromisoformat(end),
        "execution_seconds": exec_s, "dpu_seconds": dpu, "raw_row_count": raw, "gold_row_count": gold,
        "status": status, "error_message": None,
    }


def test_collect_runs_groups_by_full_run_id_and_sums_metrics():
    rows = [
        _row(1, "jr-stg-1", "mimic4-dw-etl-test-stg-load", "staging", "patients", "staging",
             "2026-01-01T00:00:00", "2026-01-01T00:01:00", 60.0, dpu=2.0, gold=100),
        _row(1, "jr-dim-1", "mimic4-dw-etl-test-wh-dimensions", "dim", "dim_patient", "dimensions",
             "2026-01-01T00:01:05", "2026-01-01T00:02:05", 60.0, dpu=4.0, raw=100, gold=100),
        # Second table from the SAME dimensions job run -- must not double-count DPU seconds.
        _row(1, "jr-dim-1", "mimic4-dw-etl-test-wh-dimensions", "dim", "dim_provider", "dimensions",
             "2026-01-01T00:02:05", "2026-01-01T00:02:35", 30.0, dpu=4.0, raw=50, gold=50),
        _row(2, "jr-stg-2", "mimic4-dw-etl-test-stg-load", "staging", "patients", "staging",
             "2026-02-01T00:00:00", "2026-02-01T00:01:30", 90.0, dpu=3.0, gold=100),
    ]

    runs = mar.collect_runs(rows)

    assert [r["full_run_id"] for r in runs] == [1, 2]
    run1 = runs[0]
    assert run1["job_count"] == 3
    assert run1["succeeded_count"] == 3
    assert run1["failed_count"] == 0
    # jr-dim-1's DPU seconds counted once even though it covers 2 table-load rows.
    assert run1["total_dpu_seconds"] == 2.0 + 4.0
    assert run1["glue_cost_usd"] == (6.0 / 3600) * mar.GLUE_DPU_HOUR_PRICE
    assert run1["total_raw_rows"] == 150
    assert run1["total_gold_rows"] == 250
    assert run1["started_at"] == "2026-01-01T00:00:00"
    assert run1["completed_at"] == "2026-01-01T00:02:35"
    assert run1["wall_clock_seconds"] == 155.0
    assert set(run1["by_process_type"]) == {"staging", "dim"}
    assert run1["by_process_type"]["dim"]["count"] == 2

    run2 = runs[1]
    assert run2["job_count"] == 1
    assert run2["total_dpu_seconds"] == 3.0


def test_collect_runs_handles_missing_row_counts_failed_jobs_and_unbackfilled_dpu():
    rows = [
        _row(5, "jr-fact-1", "mimic4-dw-etl-test-wh-facts", "fact", "fact_admission", "facts",
             "2026-01-01T00:00:00", "2026-01-01T00:01:00", 60.0, gold=200),
        # dpu_seconds still NULL -- the job run hasn't reached a terminal state yet, or
        # the backfill hasn't run; must not be treated as 0.
        _row(5, None, "mimic4-dw-etl-test-wh-facts", "fact", "fact_diagnosis", "facts",
             "2026-01-01T00:01:00", "2026-01-01T00:16:00", 900.0, status="FAILED"),
    ]

    runs = mar.collect_runs(rows)

    assert len(runs) == 1
    r = runs[0]
    assert r["failed_count"] == 1
    assert r["succeeded_count"] == 1
    assert r["total_dpu_seconds"] is None  # no row had one
    assert r["glue_cost_usd"] is None
    assert r["total_raw_rows"] is None  # neither row has one
    assert r["total_gold_rows"] == 200


def test_stats_mean_median_stdev():
    d = mar._stats([10.0, 20.0, 30.0])
    assert d == {"n": 3, "mean": 20.0, "median": 20.0, "stdev": 10.0, "min": 10.0, "max": 30.0}


def test_stats_single_value_has_no_stdev():
    d = mar._stats([42.0])
    assert d["n"] == 1
    assert d["stdev"] is None


def test_stats_ignores_none_values():
    d = mar._stats([10.0, None, 30.0])
    assert d["n"] == 2
    assert d["mean"] == 20.0


def test_compute_run_statistics_covers_every_headline_metric():
    runs = [
        {"wall_clock_seconds": 100.0, "total_dpu_seconds": 10.0, "total_execution_seconds": 90.0,
         "glue_cost_usd": 0.001, "job_count": 5, "failed_count": 0,
         "total_raw_rows": 900, "total_gold_rows": 1000},
        {"wall_clock_seconds": 200.0, "total_dpu_seconds": 20.0, "total_execution_seconds": 180.0,
         "glue_cost_usd": 0.002, "job_count": 6, "failed_count": 1,
         "total_raw_rows": 1800, "total_gold_rows": 2000},
    ]
    stats = mar.compute_run_statistics(runs)
    assert stats["n_runs"] == 2
    assert stats["wall_clock_seconds"]["mean"] == 150.0
    assert stats["job_count"]["median"] == 5.5
    assert stats["total_gold_rows"]["max"] == 2000


def test_render_runs_md_includes_detail_and_statistics_sections():
    report = {
        "generated_utc": "2026-01-01T00:00:00+00:00",
        "dataset": "demodataset",
        "runs": [{
            "full_run_id": 1, "started_at": "2026-01-01T00:00:00", "completed_at": "2026-01-01T00:05:00",
            "wall_clock_seconds": 300.0, "job_count": 2, "succeeded_count": 2, "failed_count": 0,
            "total_dpu_seconds": 6.0, "total_execution_seconds": 120.0, "glue_cost_usd": 0.0007,
            "total_raw_rows": 900000, "total_gold_rows": 910655,
            "by_process_type": {"dim": {"count": 1, "execution_seconds": 60.0},
                                "fact": {"count": 1, "execution_seconds": 60.0}},
            "rows": [],
        }],
        "summary_statistics": mar.compute_run_statistics([{
            "wall_clock_seconds": 300.0, "total_dpu_seconds": 6.0, "total_execution_seconds": 120.0,
            "glue_cost_usd": 0.0007, "job_count": 2, "failed_count": 0,
            "total_raw_rows": 900000, "total_gold_rows": 910655,
        }]),
    }
    md = mar.render_runs_md(report)
    assert "# Data warehouse pipeline -- resource usage by full run" in md
    assert "## Detailed data" in md
    assert "## Statistical analysis across runs" in md
    assert "full_run_id 1" in md
    assert "910,655" in md
    # single run -> stdev column should read as an em dash, not a fabricated 0
    assert "—" in md


def test_human_seconds_formats_by_magnitude():
    assert mar.human_seconds(45) == "45s"
    assert mar.human_seconds(125) == "2m05s"
    assert mar.human_seconds(3725) == "1h02m"
    assert mar.human_seconds(None) == "?"
