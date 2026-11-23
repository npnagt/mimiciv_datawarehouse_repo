"""Measure the AWS resource footprint of the whole Glue-based ETL pipeline -- everything
`mimic-load glue deploy` / `build_datawarehouse.py` provisioned, not just what's inside
the database (see measure_warehouse_size.py for that).

Covers, by service:
  - Aurora:          current config (engine, Serverless v2 min/max ACU, backup
                     retention), current total storage (CloudWatch VolumeBytesUsed --
                     the whole cluster volume, all databases on it), and ACU-hours
                     consumed over a time window (CloudWatch ServerlessDatabaseCapacity,
                     integrated) -- the actual unit Aurora Serverless v2 compute is
                     billed in.
  - Glue:            each of the 5 jobs' current worker config, plus DPUSeconds summed
                     across every run in its history (from get_job_runs) -- the actual
                     unit Glue is billed in, and the only way to see it since Glue
                     doesn't expose a "total DPU-hours so far" figure directly.
  - S3:              total bucket size/object count, broken down by top-level prefix
                     (hosp/icu = raw MIMIC CSVs staged for Aurora's S3 import,
                     glue-scripts/glue-sql = job code + DDL, spark-logs = Spark UI event
                     logs).
  - CloudWatch Logs: stored bytes for the 3 /aws-glue/jobs/* log groups every Glue job
                     run writes to (output, error, and the Glue 4.0+ "logs-v2" stream).
  - IAM:             the 2 roles this pipeline created (informational only -- IAM roles
                     themselves have no storage/compute cost).

Resources are identified by name, using the same settings (`load_aws_settings`) that
provisioned them -- nothing here is tagged (see AwsSettings.glue_job_name_prefix's
docstring for why the "mimic4-dw-etl" naming convention exists: distinguishing these
resources from other projects in the same account without relying on tags).

Also computes a USD cost estimate (build_cost_estimate): one-time cumulative cost (all
runs so far), one clean pipeline run (min successful DPU-seconds per job), steady-state
monthly cost (Aurora storage + CloudWatch log storage), and a combined "load the
environment once + run it for a year" projection -- mirrors the lakehouse repo's
measure_aws_resources.py cost model so the two are directly comparable via
compare_environments.py.

Two modes:

  Point-in-time snapshot (default) -- current Aurora/Glue/S3/CloudWatch/IAM state plus
  the cost estimate above, as of right now.

  --by-run -- per-table resource/timing detail from warehouse.etl_process_log grouped by
  full_run_id, restricted to the last --last-n runs, plus mean/median/stdev/min/max
  across them and a current-footprint snapshot (Aurora/Glue/S3/CloudWatch/cost) for
  context -- see collect_runs / collect_current_footprint. Written to
  docs/cost/<demo|full>/runs_<UTC>.{json,md} + latest_runs.{json,md}.

Usage:
    python scripts/measure_aws_resources.py [OPTIONS]

Options:
    --dataset {demodataset,fulldataset}
                        Dataset whose resources to inventory (default: demodataset,
                        or the MIMIC_DATASET env var). The Aurora cluster is shared
                        across datasets, so its ACU-hours/volume cover both.
    --detail            Also print per-Glue-job-run detail (point-in-time snapshot
                        mode only; ignored with --by-run).
    --hours HOURS       ACU-hours lookback window in hours, for both the point-in-time
                        snapshot's Aurora section and --by-run's current-footprint
                        Aurora section (default: since the Aurora cluster was created).
    --json              Print a machine-readable JSON report instead of text
                        (point-in-time snapshot mode only; --by-run always writes JSON
                        + Markdown files regardless of this flag).
    --by-run            Read warehouse.etl_process_log instead of the point-in-time
                        account-wide snapshot -- see "Two modes" above.
    --last-n N          With --by-run: restrict the detailed rows and summary
                        statistics to the most recent N full_run_ids (default: 5).
                        Pass 0 for every run found.
    --since TIMESTAMP   With --by-run: exclude any full_run_id that started before
                        this timestamp ('YYYY-MM-DD HH:MM:SS[.ffffff]' or ISO 8601;
                        naive values are treated as UTC). Applied before --last-n,
                        so you get the last N runs *after* this cutoff -- e.g. to
                        exclude runs affected by a bug that has since been fixed.

Examples:
    python scripts/measure_aws_resources.py                          # demodataset summary
    python scripts/measure_aws_resources.py --dataset fulldataset     # fulldataset summary
    python scripts/measure_aws_resources.py --detail                  # + per-run detail
    python scripts/measure_aws_resources.py --hours 168                # ACU-hours over the last week
    python scripts/measure_aws_resources.py --json
    python scripts/measure_aws_resources.py --dataset fulldataset --by-run --last-n 5
    python scripts/measure_aws_resources.py --dataset fulldataset --by-run --last-n 0  # every run ever
    python scripts/measure_aws_resources.py --dataset fulldataset --by-run --last-n 5 \
        --since "2026-09-19 17:41:02.235776"                          # exclude pre-fix runs
"""

from __future__ import annotations

import argparse
import json
import os
import statistics
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import boto3
from sqlalchemy import text

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from mimic_datawarehouse.aws.glue import GLUE_JOB_STEPS, job_name
from mimic_datawarehouse.config import DATASET_CHOICES, dataset_profile, load_aws_settings, load_settings
from mimic_datawarehouse.db import make_engine

# USD per Glue DPU-hour -- matches the lakehouse repo's measure_aws_resources.py
# PRICES["glue_dpu_hour"], kept in sync manually since these are separate repos.
GLUE_DPU_HOUR_PRICE = 0.44

# Aurora PostgreSQL Serverless v2, Standard (I/O-included-per-request) storage,
# us-east-2 public on-demand pricing -- the same rates compare_environments.py's build()
# previously hand-computed once and pasted in as a fixed approximation; sourced here from
# live CloudWatch metrics instead (see aurora_report) so the number tracks actual usage.
AURORA_ACU_HOUR_PRICE = 0.12               # per Aurora Capacity Unit-hour
AURORA_STORAGE_GB_MONTH_PRICE = 0.10       # per GB-month of cluster volume
AURORA_IO_PER_MILLION_PRICE = 0.20         # per 1,000,000 billed I/O requests

# Matches the lakehouse repo's measure_aws_resources.py PRICES of the same names/units.
GB = 10 ** 9
CW_LOGS_INGEST_PER_GB = 0.50
CW_LOGS_STORAGE_GB_MONTH = 0.03

GLUE_LOG_GROUPS = [
    "/aws-glue/jobs/output",
    "/aws-glue/jobs/error",
    "/aws-glue/jobs/logs-v2",
]


def usd(x: float) -> float:
    return round(float(x), 4)


def _parse_ts(s):
    """Parse a timestamp that may come from Postgres (a datetime's .isoformat(), naive or
    tz-aware) or a --since CLI value in either that format or 'YYYY-MM-DD HH:MM:SS[.ffffff]'.
    Returns a naive UTC datetime. Mirrors the lakehouse repo's measure_aws_resources._parse_ts."""
    if not s:
        return None
    s = s.strip()
    if " " in s and "T" not in s:
        s = s.replace(" ", "T", 1)
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt


def pretty_bytes(n: float) -> str:
    for unit, size in (("GiB", 1024**3), ("MiB", 1024**2), ("KiB", 1024)):
        if n >= size:
            return f"{n / size:.2f} {unit}"
    return f"{n:.0f} B"


def _cloudwatch_period_seconds(window_seconds: float) -> int:
    """CloudWatch's GetMetricStatistics caps a single call at 1440 datapoints. Pick the
    smallest 60s-aligned period that keeps this window under that, floored at 60s (the
    finest resolution available once a metric is more than a few hours old)."""
    period = max(60, int(window_seconds / 1440))
    return period - (period % 60) + (60 if period % 60 else 0)


def aurora_report(rds, cloudwatch, cluster_id: str, hours: float | None) -> dict:
    cluster = rds.describe_db_clusters(DBClusterIdentifier=cluster_id)["DBClusters"][0]
    create_time: datetime = cluster["ClusterCreateTime"]
    now = datetime.now(timezone.utc)
    start_time = now - timedelta(hours=hours) if hours else create_time
    window_seconds = (now - start_time).total_seconds()
    period = _cloudwatch_period_seconds(window_seconds)

    def metric(name: str, stat: str) -> list[dict]:
        return cloudwatch.get_metric_statistics(
            Namespace="AWS/RDS",
            MetricName=name,
            Dimensions=[{"Name": "DBClusterIdentifier", "Value": cluster_id}],
            StartTime=start_time,
            EndTime=now,
            Period=period,
            Statistics=[stat],
        )["Datapoints"]

    volume_points = sorted(
        metric("VolumeBytesUsed", "Average"), key=lambda d: d["Timestamp"]
    )
    current_volume_bytes = volume_points[-1]["Average"] if volume_points else None

    acu_points = metric("ServerlessDatabaseCapacity", "Average")
    acu_hours = sum(p["Average"] * period / 3600 for p in acu_points)
    max_acu_observed = max((p["Average"] for p in acu_points), default=None)

    # Billed I/O requests over the same window (Aurora Standard storage bills these
    # separately from ACU-hours and from the GB-month storage rate) -- Sum, not Average,
    # since each datapoint is a count of requests in that period, not a level.
    read_iops = sum(p["Sum"] for p in metric("VolumeReadIOPs", "Sum"))
    write_iops = sum(p["Sum"] for p in metric("VolumeWriteIOPs", "Sum"))
    io_operations = read_iops + write_iops

    acu_cost_usd = acu_hours * AURORA_ACU_HOUR_PRICE
    io_cost_usd = io_operations / 1_000_000 * AURORA_IO_PER_MILLION_PRICE
    storage_cost_usd_per_month = (
        (current_volume_bytes / 10**9) * AURORA_STORAGE_GB_MONTH_PRICE
        if current_volume_bytes is not None else None
    )

    scaling = cluster.get("ServerlessV2ScalingConfiguration", {})
    return {
        "cluster_id": cluster_id,
        "engine": f"{cluster['Engine']} {cluster['EngineVersion']}",
        "min_acu": scaling.get("MinCapacity"),
        "max_acu": scaling.get("MaxCapacity"),
        "backup_retention_days": cluster.get("BackupRetentionPeriod"),
        "created": create_time.isoformat(),
        "window_start": start_time.isoformat(),
        "current_volume_bytes": current_volume_bytes,
        "acu_hours_in_window": acu_hours,
        "max_acu_observed_in_window": max_acu_observed,
        "io_operations_in_window": io_operations,
        # Compute + I/O are both genuinely "cost incurred over window_start..now"; storage
        # is a separate, ongoing monthly rate against the CURRENT volume, not a windowed
        # cost -- kept as a distinct field rather than summed into the others so a caller
        # never accidentally adds a monthly rate to a windowed one.
        "acu_cost_usd_in_window": round(acu_cost_usd, 4),
        "io_cost_usd_in_window": round(io_cost_usd, 4),
        "compute_and_io_cost_usd_in_window": round(acu_cost_usd + io_cost_usd, 4),
        "storage_cost_usd_per_month": (
            round(storage_cost_usd_per_month, 4) if storage_cost_usd_per_month is not None else None
        ),
    }


def glue_report(glue_client, job_name_prefix: str) -> dict:
    jobs = []
    for step in GLUE_JOB_STEPS:
        name = job_name(job_name_prefix, step)
        try:
            config = glue_client.get_job(JobName=name)["Job"]
        except glue_client.exceptions.EntityNotFoundException:
            continue

        total_dpu_seconds = 0.0
        total_execution_seconds = 0
        run_states: dict[str, int] = {}
        runs = []
        paginator = glue_client.get_paginator("get_job_runs")
        for page in paginator.paginate(JobName=name):
            for run in page["JobRuns"]:
                total_dpu_seconds += run.get("DPUSeconds", 0.0)
                total_execution_seconds += run.get("ExecutionTime", 0)
                run_states[run["JobRunState"]] = (
                    run_states.get(run["JobRunState"], 0) + 1
                )
                runs.append(
                    {
                        "id": run["Id"],
                        "state": run["JobRunState"],
                        "started": run["StartedOn"].isoformat(),
                        "execution_seconds": run.get("ExecutionTime", 0),
                        "dpu_seconds": run.get("DPUSeconds", 0.0),
                    }
                )

        # Minimum DPU-seconds among this job's SUCCEEDED runs -- mirrors the lakehouse
        # repo's measure_aws_resources.py collect_glue_jobs' dpu_seconds_one_clean_run:
        # the cheapest a single successful load of this table has actually cost, used as
        # the "one clean pipeline run" unit price rather than summing every run (including
        # failed/debugging ones) ever executed against it.
        ok_dpu = [r["dpu_seconds"] for r in runs if r["state"] == "SUCCEEDED"]
        jobs.append(
            {
                "step": step,
                "name": name,
                "worker_type": config.get("WorkerType"),
                "number_of_workers": config.get("NumberOfWorkers"),
                "max_capacity": config.get("MaxCapacity"),
                "glue_version": config.get("GlueVersion"),
                "run_count": len(runs),
                "run_states": run_states,
                "total_execution_seconds": total_execution_seconds,
                "total_dpu_seconds": total_dpu_seconds,
                "dpu_seconds_one_clean_run": min(ok_dpu) if ok_dpu else None,
                "runs": runs,
            }
        )
    return {
        "jobs": jobs,
        "total_dpu_hours": sum(j["total_dpu_seconds"] for j in jobs) / 3600,
        "total_dpu_hours_one_clean_pipeline": sum((j["dpu_seconds_one_clean_run"] or 0) for j in jobs) / 3600,
    }


def s3_report(s3_client, bucket: str) -> dict:
    prefixes: dict[str, dict] = {}
    total_bytes = 0
    total_objects = 0
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket):
        for obj in page.get("Contents", []):
            top_prefix = obj["Key"].split("/", 1)[0] if "/" in obj["Key"] else "(root)"
            bucket_stats = prefixes.setdefault(top_prefix, {"bytes": 0, "objects": 0})
            bucket_stats["bytes"] += obj["Size"]
            bucket_stats["objects"] += 1
            total_bytes += obj["Size"]
            total_objects += 1
    return {
        "bucket": bucket,
        "total_bytes": total_bytes,
        "total_objects": total_objects,
        "prefixes": prefixes,
    }


def logs_report(logs_client) -> dict:
    groups = []
    for name in GLUE_LOG_GROUPS:
        resp = logs_client.describe_log_groups(logGroupNamePrefix=name)
        for group in resp["logGroups"]:
            if group["logGroupName"] == name:
                groups.append(
                    {"name": name, "stored_bytes": group.get("storedBytes", 0)}
                )
    return {"log_groups": groups, "total_bytes": sum(g["stored_bytes"] for g in groups)}


def iam_report(iam_client, role_names: list[str]) -> dict:
    roles = []
    for name in role_names:
        try:
            role = iam_client.get_role(RoleName=name)["Role"]
            roles.append(
                {
                    "name": name,
                    "arn": role["Arn"],
                    "created": role["CreateDate"].isoformat(),
                }
            )
        except iam_client.exceptions.NoSuchEntityException:
            continue
    return {"roles": roles}


def build_cost_estimate(aurora: dict, glue_data: dict, logs_data: dict) -> dict:
    """USD cost estimate for the warehouse pipeline, mirroring the lakehouse repo's
    measure_aws_resources.py build_cost() three-tier structure (one_time_cumulative /
    one_clean_pipeline_run / steady_state_monthly) plus an annual projection, so the two
    repos' cost_estimate_usd blocks are directly comparable. Aurora's ACU/I/O cost is a
    genuine per-window figure (see aurora_report); Glue's "one clean run" price uses the
    minimum successful DPU-seconds per job (see glue_report), the same convention the
    lakehouse side uses, rather than summing every historical run including failures and
    debugging."""
    glue_all_usd = usd(glue_data["total_dpu_hours"] * GLUE_DPU_HOUR_PRICE)
    glue_clean_usd = usd(glue_data["total_dpu_hours_one_clean_pipeline"] * GLUE_DPU_HOUR_PRICE)
    cw_gb = logs_data["total_bytes"] / GB
    cw_ingest_usd = usd(cw_gb * CW_LOGS_INGEST_PER_GB)
    cw_store_usd = usd(cw_gb * CW_LOGS_STORAGE_GB_MONTH)
    storage_usd = aurora["storage_cost_usd_per_month"] or 0.0

    one_time = usd(aurora["compute_and_io_cost_usd_in_window"] + glue_all_usd + cw_ingest_usd)
    # Aurora's ACU/I/O cost isn't separable into "cost of one pipeline run" without
    # correlating CloudWatch datapoints to individual Glue job-run windows -- reported
    # here as Glue-only, same limitation the lakehouse side has for its crawler-adjacent
    # costs (Athena DDL, catalog) in its own one_clean_pipeline_run figure.
    one_run = glue_clean_usd
    steady = usd(storage_usd + cw_store_usd)

    return {
        "one_time_cumulative": {
            "aurora_compute_and_io": aurora["compute_and_io_cost_usd_in_window"],
            "glue_etl_all_runs": glue_all_usd,
            "cloudwatch_log_ingestion": cw_ingest_usd,
            "total": one_time,
        },
        "one_clean_pipeline_run": {
            "glue_etl_min_successful_run_per_job": glue_clean_usd,
            "total_min": one_run,
            "note": "Aurora ACU/I/O cost is not separable per-run from the account-wide "
                    "CloudWatch window -- this figure is Glue-only.",
        },
        "steady_state_monthly": {
            "aurora_storage": round(storage_usd, 4),
            "cloudwatch_log_storage": cw_store_usd,
            "total": steady,
        },
        "annual_cost_estimate": {
            "load_cost_usd": one_run,
            "annual_steady_state_usd": usd(steady * 12),
            "first_year_total_usd": usd(one_run + steady * 12),
            "notes": [
                "load_cost_usd = one_clean_pipeline_run.total_min (Glue-only; see its note).",
                "annual_steady_state = 12 x steady_state_monthly -- assumes the Aurora cluster "
                "genuinely scales to its configured min ACU (0 if set, otherwise a sustained "
                "floor cost applies that this estimate does not include) and no further "
                "pipeline runs during the year.",
            ],
        },
    }


def _pg_process_log_rows(settings) -> list[dict]:
    """Every warehouse.etl_process_log row with a full_run_id set (i.e. logged by
    glue_common.log_timed_call, not a pre-instrumentation run), oldest first. Column set
    mirrors the lakehouse's etl_process_log -- see warehouse.py's table comment.
    dpu_seconds is backfilled by cli.py's _run_glue_step once each row's Glue job run is
    terminal, so it's read directly here rather than re-derived via the Glue API."""
    engine = make_engine(settings)
    with engine.connect() as conn:
        result = conn.execute(
            text(
                "SELECT full_run_id, run_id, job_name, process_name, process_type, phase, "
                "dataset, start_ts, end_ts, execution_seconds, dpu_seconds, raw_row_count, "
                "gold_row_count, status, error_message "
                "FROM warehouse.etl_process_log WHERE full_run_id IS NOT NULL "
                "ORDER BY full_run_id, start_ts"
            )
        )
        return [dict(row._mapping) for row in result]


def cloudwatch_bytes_ingested(cloudwatch, log_groups, start, end) -> float | None:
    """Total AWS/Logs IncomingBytes summed across `log_groups` for [start, end] -- the
    CloudWatch log volume ONE run actually generated. Deliberately not storedBytes (a
    point-in-time total for the whole log group's retention, with no notion of "as of run
    N") -- IncomingBytes is a genuine per-period metric available within minutes, so it's
    safe to use even for a run that just finished, unlike S3/EBS daily storage metrics
    which lag by up to a day. Mirrors the lakehouse repo's measure_aws_resources.py
    function of the same name. Returns None if the run has no recorded window."""
    if start is None or end is None:
        return None
    if start.tzinfo is None:
        start = start.replace(tzinfo=timezone.utc)
    if end.tzinfo is None:
        end = end.replace(tzinfo=timezone.utc)
    end = end + timedelta(minutes=5)  # Glue's log delivery lags job completion slightly
    total = 0.0
    for lg in log_groups:
        try:
            resp = cloudwatch.get_metric_statistics(
                Namespace="AWS/Logs", MetricName="IncomingBytes",
                Dimensions=[{"Name": "LogGroupName", "Value": lg}],
                StartTime=start, EndTime=end, Period=300, Statistics=["Sum"],
            )
            total += sum(p["Sum"] for p in resp.get("Datapoints", []))
        except Exception:  # noqa: BLE001 -- a missing/inaccessible log group shouldn't abort the report
            continue
    return total


def cloudwatch_bytes_attributed(logs_client, log_groups, run_ids, start, end) -> float | None:
    """Proportional share of a shared Glue log group's ingested bytes that belongs to a
    specific set of Glue JobRunIds, via CloudWatch Logs Insights. Mirrors the lakehouse
    repo's measure_aws_resources.cloudwatch_bytes_attributed exactly -- see its docstring
    for why this is needed: the account's 7 Glue-managed log groups are shared between
    this pipeline and the lakehouse's, DescribeLogStreams' storedBytes is deprecated
    (always 0), but every log stream is named jr_<JobRunId>[-task], so a job's own
    JobRunId (already captured per row as run_id) can be matched against @logStream for
    exact per-job-run attribution. Approximates ingested bytes via message length
    (strlen(@message)), not CloudWatch's own wire-format IncomingBytes accounting.
    Returns None if run_ids is empty or every query fails."""
    if not run_ids:
        return None
    if start is None or end is None:
        return None
    if start.tzinfo is None:
        start = start.replace(tzinfo=timezone.utc)
    if end.tzinfo is None:
        end = end.replace(tzinfo=timezone.utc)
    end = end + timedelta(minutes=5)  # Glue's log delivery lags job completion slightly
    pattern = "|".join(sorted(run_ids))
    query = f"filter @logStream like /^({pattern})(-|$)/ | stats sum(strlen(@message)) as bytes"
    total = 0.0
    any_ok = False
    for lg in log_groups:
        try:
            qid = logs_client.start_query(
                logGroupName=lg, startTime=int(start.timestamp()), endTime=int(end.timestamp()),
                queryString=query, limit=1,
            )["queryId"]
        except Exception:  # noqa: BLE001
            continue
        res = None
        for _ in range(30):
            res = logs_client.get_query_results(queryId=qid)
            if res["status"] in ("Complete", "Failed", "Cancelled"):
                break
            time.sleep(1)
        if res is None or res["status"] != "Complete" or not res.get("results"):
            continue
        any_ok = True
        for field in res["results"][0]:
            if field["field"] == "bytes":
                try:
                    total += float(field["value"])
                except (TypeError, ValueError):
                    pass
    return total if any_ok else None


def collect_current_footprint(rds, cloudwatch, glue_client, s3_client, logs_client, iam_client,
                              settings, hours: float | None) -> dict:
    """Point-in-time context attached to a --by-run report: current Aurora config/storage,
    Glue job config, S3 bucket size, CloudWatch log totals, and IAM roles -- NOT per-run
    (S3/Aurora storage metrics are daily-to-hourly and lag, so retroactively attributing
    "storage as of run N" would be fabricated precision). Reuses the snapshot-mode
    collectors so there's one source of truth for what each figure means."""
    aurora = aurora_report(rds, cloudwatch, settings.aurora_cluster_id, hours)
    glue_data = glue_report(glue_client, settings.glue_job_name_prefix)
    logs_data = logs_report(logs_client)
    return {
        "measured_at": datetime.now(timezone.utc).isoformat(),
        "aurora": aurora,
        "glue": glue_data,
        "s3": s3_report(s3_client, settings.s3_bucket),
        "cloudwatch_logs": logs_data,
        "iam": iam_report(iam_client, [settings.iam_role_name, settings.glue_role_name]),
        "cost_estimate_usd": build_cost_estimate(aurora, glue_data, logs_data),
    }


def collect_runs(pg_rows: list[dict], cloudwatch=None, logs_client=None) -> list[dict]:
    """Group warehouse.etl_process_log rows by full_run_id and summarize each run: wall
    clock time, job (table-load) counts, total Glue DPU-seconds/cost, and summed
    raw/gold row counts. Mirrors the lakehouse repo's measure_aws_resources.collect_runs
    exactly, adapted to this repo's per-table (not per-job) logging grain and Postgres
    (not Athena) backing store."""
    by_run: dict[int, list[dict]] = {}
    for row in pg_rows:
        by_run.setdefault(row["full_run_id"], []).append(row)

    runs = []
    for full_run_id in sorted(by_run):
        rows = by_run[full_run_id]
        started_at = min(r["start_ts"] for r in rows if r["start_ts"] is not None)
        completed_at = max(r["end_ts"] for r in rows if r["end_ts"] is not None)

        def _sum_if_any(col: str, _rows=rows):
            values = [r[col] for r in _rows if r[col] is not None]
            return sum(values) if values else None

        # dpu_seconds is identical across every row sharing one run_id (backfilled once
        # per completed Glue job run) -- sum it once per distinct run_id, not once per
        # table row, or a job that loaded 20 tables would count its DPU-seconds 20x.
        seen_run_ids: set[str] = set()
        total_dpu_seconds = 0.0
        any_dpu = False
        for r in rows:
            if r["run_id"] in seen_run_ids:
                continue
            seen_run_ids.add(r["run_id"])
            if r["dpu_seconds"] is not None:
                total_dpu_seconds += r["dpu_seconds"]
                any_dpu = True

        by_process_type: dict[str, dict] = {}
        for r in rows:
            bucket = by_process_type.setdefault(r["process_type"], {"count": 0, "execution_seconds": 0.0})
            bucket["count"] += 1
            bucket["execution_seconds"] += r["execution_seconds"] or 0.0

        cw_bytes = (cloudwatch_bytes_ingested(cloudwatch, GLUE_LOG_GROUPS, started_at, completed_at)
                   if cloudwatch is not None else None)
        cw_bytes_attributed = (
            cloudwatch_bytes_attributed(logs_client, GLUE_LOG_GROUPS, seen_run_ids, started_at, completed_at)
            if logs_client is not None else None
        )

        runs.append(
            {
                "full_run_id": full_run_id,
                "dataset": rows[0].get("dataset"),
                "started_at": started_at.isoformat() if started_at else None,
                "completed_at": completed_at.isoformat() if completed_at else None,
                "wall_clock_seconds": (completed_at - started_at).total_seconds() if started_at and completed_at else None,
                "job_count": len(rows),
                "succeeded_count": sum(1 for r in rows if r["status"] == "SUCCEEDED"),
                "failed_count": sum(1 for r in rows if r["status"] == "FAILED"),
                "total_execution_seconds": sum(r["execution_seconds"] or 0.0 for r in rows),
                "total_dpu_seconds": total_dpu_seconds if any_dpu else None,
                "total_dpu_hours": (total_dpu_seconds / 3600) if any_dpu else None,
                "glue_cost_usd": (total_dpu_seconds / 3600 * GLUE_DPU_HOUR_PRICE) if any_dpu else None,
                "cloudwatch_bytes_ingested": round(cw_bytes, 0) if cw_bytes is not None else None,
                "cloudwatch_bytes_attributed": round(cw_bytes_attributed, 0) if cw_bytes_attributed is not None else None,
                "total_raw_rows": _sum_if_any("raw_row_count"),
                "total_gold_rows": _sum_if_any("gold_row_count"),
                "by_process_type": by_process_type,
                "rows": rows,
            }
        )
    return runs


def _stats(values: list[float]) -> dict:
    clean = [v for v in values if v is not None]
    if not clean:
        return {"n": 0, "mean": None, "median": None, "stdev": None, "min": None, "max": None}
    return {
        "n": len(clean),
        "mean": statistics.mean(clean),
        "median": statistics.median(clean),
        "stdev": statistics.stdev(clean) if len(clean) >= 2 else None,
        "min": min(clean),
        "max": max(clean),
    }


def compute_run_statistics(runs: list[dict]) -> dict:
    metrics = [
        "wall_clock_seconds", "total_dpu_seconds", "total_dpu_hours", "total_execution_seconds",
        "glue_cost_usd", "cloudwatch_bytes_ingested", "cloudwatch_bytes_attributed",
        "job_count", "failed_count", "total_raw_rows", "total_gold_rows",
    ]
    return {"n_runs": len(runs), **{m: _stats([r.get(m) for r in runs]) for m in metrics}}


def human_seconds(s: float | None) -> str:
    if s is None:
        return "?"
    s = int(s)
    if s < 60:
        return f"{s}s"
    if s < 3600:
        return f"{s // 60}m{s % 60:02d}s"
    return f"{s // 3600}h{(s % 3600) // 60:02d}m"


def render_runs_md(report: dict) -> str:
    lines = [
        "# Data warehouse pipeline -- resource usage by full run",
        "",
        f"Generated {report['generated_utc']} UTC. Dataset: {report['dataset']}. "
        f"Last {report.get('last_n', report['summary_statistics']['n_runs'])} run(s) "
        f"of {report.get('n_runs_available', report['summary_statistics']['n_runs'])} found.",
    ]
    if report.get("since"):
        lines.append(
            f"- **Since cutoff:** `{report['since']}` -- "
            f"{report.get('n_runs_excluded_before_since', 0)} run(s) excluded as started before it "
            f"({report.get('n_runs_eligible', '?')} of {report.get('n_runs_available', '?')} eligible)"
        )
    lines += [
        "",
        "## Detailed data",
        "",
    ]
    for run in report["runs"]:
        lines.append(f"### full_run_id {run['full_run_id']}")
        lines.append("")
        lines.append(f"- Started: {run['started_at']}  Completed: {run['completed_at']}")
        lines.append(
            f"- Wall clock: {human_seconds(run['wall_clock_seconds'])}  "
            f"Tables loaded: {run['job_count']} ({run['succeeded_count']} succeeded, {run['failed_count']} failed)"
        )
        dpu = run["total_dpu_seconds"]
        cost = run["glue_cost_usd"]
        lines.append(
            f"- Glue: {dpu:.2f} DPU-sec (${cost:.4f})" if dpu is not None else "- Glue: no DPUSeconds available"
        )
        cw = run.get("cloudwatch_bytes_ingested")
        lines.append(f"- CloudWatch bytes ingested (shared log group total): {pretty_bytes(cw)}"
                     if cw is not None else "- CloudWatch (shared log group total): n/a")
        cw_attr = run.get("cloudwatch_bytes_attributed")
        lines.append(f"- CloudWatch bytes attributed (this run's own jobs): {pretty_bytes(cw_attr)}"
                     if cw_attr is not None else "- CloudWatch (attributed to this run): n/a")
        raw, gold = run["total_raw_rows"], run["total_gold_rows"]
        lines.append(
            f"- Rows: raw {raw:,}  gold {gold:,}"
            if raw is not None and gold is not None
            else f"- Gold rows: {gold:,}" if gold is not None else "- Rows: n/a"
        )
        lines.append("")
        lines.append("| process_type | tables | execution_seconds |")
        lines.append("|---|---|---|")
        for ptype, stats in sorted(run["by_process_type"].items()):
            lines.append(f"| {ptype} | {stats['count']} | {stats['execution_seconds']:.1f} |")
        lines.append("")

    lines.append("## Statistical analysis across runs")
    lines.append("")
    stats = report["summary_statistics"]
    lines.append(f"n = {stats['n_runs']} run(s).")
    lines.append("")
    lines.append("| metric | mean | median | stdev | min | max |")
    lines.append("|---|---|---|---|---|---|")
    for metric in (
        "wall_clock_seconds", "total_dpu_seconds", "total_dpu_hours", "total_execution_seconds",
        "glue_cost_usd", "cloudwatch_bytes_ingested", "cloudwatch_bytes_attributed",
        "job_count", "failed_count", "total_raw_rows", "total_gold_rows",
    ):
        s = stats[metric]

        def _fmt(v):
            return "—" if v is None else f"{v:,.3f}" if isinstance(v, float) else f"{v:,}"

        lines.append(f"| {metric} | {_fmt(s['mean'])} | {_fmt(s['median'])} | {_fmt(s['stdev'])} | {_fmt(s['min'])} | {_fmt(s['max'])} |")

    fp = report.get("current_footprint")
    if fp:
        lines.append("")
        lines.append("## Current resource footprint (point-in-time, not per-run)")
        lines.append("")
        lines.append(
            "Aurora storage, S3, and CloudWatch log retention don't have a clean per-run "
            "reading (these metrics are daily-to-hourly and lag) -- this is the current "
            "state, for context alongside the per-run compute/logging trend above."
        )
        lines.append("")
        a, g, s3, cw, iam = fp["aurora"], fp["glue"], fp["s3"], fp["cloudwatch_logs"], fp["iam"]
        vol = a["current_volume_bytes"]
        lines.append(f"- **Measured:** {fp['measured_at']}")
        storage_cost = a["storage_cost_usd_per_month"]
        lines.append(
            f"- **Aurora** `{a['cluster_id']}` ({a['engine']}): "
            f"{pretty_bytes(vol) if vol is not None else 'no datapoints'} volume, "
            f"min/max ACU {a['min_acu']}/{a['max_acu']}, "
            f"{a['acu_hours_in_window']:.3f} ACU-hr since {a['window_start'][:10]}"
        )
        lines.append(
            f"- **Aurora cost**: ${a['compute_and_io_cost_usd_in_window']:.4f} compute+I/O "
            f"(${a['acu_cost_usd_in_window']:.4f} ACU + ${a['io_cost_usd_in_window']:.4f} I/O, "
            f"{a['io_operations_in_window']:,.0f} requests) since {a['window_start'][:10]}, plus "
            + (f"${storage_cost:.4f}/month storage at current volume"
               if storage_cost is not None else "storage cost n/a (no volume datapoints)")
        )
        lines.append(f"- **Glue**: {g['total_dpu_hours']:.4f} total DPU-hours across {len(g['jobs'])} job(s)")
        lines.append(f"- **S3** `{s3['bucket']}`: {pretty_bytes(s3['total_bytes'])} across {s3['total_objects']} objects")
        lines.append(f"- **CloudWatch log groups**: {pretty_bytes(cw['total_bytes'])} across {len(cw['log_groups'])} group(s)")
        lines.append(f"- **IAM roles**: {len(iam['roles'])}")
        cc = fp.get("cost_estimate_usd")
        if cc:
            ac = cc["annual_cost_estimate"]
            lines.append(
                f"- **Load + 1 year steady-state**: ${ac['first_year_total_usd']:.2f} "
                f"(load ${ac['load_cost_usd']:.4f} + annual steady-state ${ac['annual_steady_state_usd']:.2f})"
            )

    return "\n".join(lines) + "\n"


def print_report(
    aurora: dict,
    glue_data: dict,
    s3_data: dict,
    logs_data: dict,
    iam_data: dict,
    cost: dict,
    detail: bool,
) -> None:
    print("=" * 100)
    print(
        "AWS resource footprint -- Glue ETL pipeline (baseline for lakehouse comparison)"
    )
    print("=" * 100)

    print("\nAurora (RDS)")
    print("-" * 100)
    print(f"  Cluster              : {aurora['cluster_id']} ({aurora['engine']})")
    print(f"  Serverless v2 ACU    : min {aurora['min_acu']} / max {aurora['max_acu']}")
    print(f"  Created              : {aurora['created']}")
    vol = aurora["current_volume_bytes"]
    print(
        f"  Current volume used  : {pretty_bytes(vol) if vol is not None else 'no datapoints'}"
    )
    print(
        f"  ACU-hours since {aurora['window_start'][:10]}: {aurora['acu_hours_in_window']:.3f} ACU-hr"
        f" (peak {aurora['max_acu_observed_in_window']} ACU)"
    )
    print(
        f"  Compute + I/O cost   : ${aurora['compute_and_io_cost_usd_in_window']:.4f} "
        f"(${aurora['acu_cost_usd_in_window']:.4f} ACU + ${aurora['io_cost_usd_in_window']:.4f} I/O, "
        f"{aurora['io_operations_in_window']:,.0f} requests) since {aurora['window_start'][:10]}"
    )
    storage_cost = aurora["storage_cost_usd_per_month"]
    print(
        f"  Storage cost         : ${storage_cost:.4f}/month at current volume"
        if storage_cost is not None else "  Storage cost         : n/a (no volume datapoints)"
    )

    print("\nGlue jobs")
    print("-" * 100)
    print(
        f"  {'job':<28} {'workers':>10} {'runs':>6} {'exec (s)':>10} {'DPU-sec':>10} {'states'}"
    )
    for j in glue_data["jobs"]:
        workers = (
            f"{j['number_of_workers']}x{j['worker_type']}"
            if j["number_of_workers"]
            else f"{j['max_capacity']} DPU"
        )
        states = ", ".join(f"{k}:{v}" for k, v in j["run_states"].items())
        print(
            f"  {j['name']:<28} {workers:>10} {j['run_count']:>6} {j['total_execution_seconds']:>10} "
            f"{j['total_dpu_seconds']:>10.1f} {states}"
        )
    print(
        f"  {'TOTAL':<28} {'':>10} {'':>6} {'':>10} {'':>10}   {glue_data['total_dpu_hours']:.4f} DPU-hours total"
    )

    if detail:
        print("\n  Per-run detail:")
        for j in glue_data["jobs"]:
            for run in j["runs"]:
                print(
                    f"    {j['name']:<28} {run['started']}  {run['state']:<10} "
                    f"{run['execution_seconds']:>6}s  {run['dpu_seconds']:>8.1f} DPU-sec"
                )

    print("\nS3")
    print("-" * 100)
    print(f"  Bucket               : {s3_data['bucket']}")
    print(
        f"  Total                : {pretty_bytes(s3_data['total_bytes'])} across {s3_data['total_objects']} objects"
    )
    for prefix, stats in sorted(
        s3_data["prefixes"].items(), key=lambda kv: -kv[1]["bytes"]
    ):
        print(
            f"    {prefix + '/':<20} {pretty_bytes(stats['bytes']):>12}  ({stats['objects']} objects)"
        )

    print("\nCloudWatch Logs (Glue job output)")
    print("-" * 100)
    for g in logs_data["log_groups"]:
        print(f"  {g['name']:<28} {pretty_bytes(g['stored_bytes'])}")
    print(f"  {'TOTAL':<28} {pretty_bytes(logs_data['total_bytes'])}")

    print("\nIAM roles (informational -- no storage/compute cost)")
    print("-" * 100)
    for r in iam_data["roles"]:
        print(f"  {r['name']:<40} created {r['created'][:10]}")

    print("\nCost estimate (USD)")
    print("-" * 100)
    ot, orr, sm, ac = (cost["one_time_cumulative"], cost["one_clean_pipeline_run"],
                      cost["steady_state_monthly"], cost["annual_cost_estimate"])
    print(f"  One-time (cumulative, all runs) : ${ot['total']:.2f}  "
          f"(Aurora ${ot['aurora_compute_and_io']:.4f} + Glue ${ot['glue_etl_all_runs']:.2f} + "
          f"CW ingest ${ot['cloudwatch_log_ingestion']:.4f})")
    print(f"  One clean pipeline run          : ${orr['total_min']:.4f}  ({orr['note']})")
    print(f"  Steady-state / month            : ${sm['total']:.4f}  "
          f"(Aurora storage ${sm['aurora_storage']:.4f} + CW storage ${sm['cloudwatch_log_storage']:.4f})")
    print(f"  Load + 1 year steady-state      : ${ac['first_year_total_usd']:.2f}  "
          f"(load ${ac['load_cost_usd']:.4f} + annual ${ac['annual_steady_state_usd']:.2f})")

    print()
    print("For what's inside the database itself (staging/warehouse table sizes), see")
    print("scripts/measure_warehouse_size.py.")


def build_json_report(
    aurora: dict, glue_data: dict, s3_data: dict, logs_data: dict, iam_data: dict,
    cost: dict, dataset: str = "demodataset"
) -> dict:
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "dataset": dataset,
        "aurora": aurora,
        "glue": glue_data,
        "s3": s3_data,
        "cloudwatch_logs": logs_data,
        "iam": iam_data,
        "cost_estimate_usd": cost,
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--detail", action="store_true", help="Also print per-Glue-job-run detail."
    )
    parser.add_argument(
        "--hours",
        type=float,
        default=None,
        help="ACU-hours lookback window in hours (default: since the Aurora cluster was created).",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print a machine-readable JSON report instead of text.",
    )
    parser.add_argument(
        "--dataset", choices=DATASET_CHOICES, default=None,
        help="Dataset whose resources to inventory (default: demodataset, or MIMIC_DATASET). "
        "The Aurora cluster is shared, so its ACU-hours cover both datasets.",
    )
    parser.add_argument(
        "--by-run", action="store_true",
        help="Read warehouse.etl_process_log instead: detailed per-table resource/timing "
        "data grouped by full_run_id, plus mean/median/stdev across the last --last-n runs "
        "found, written to docs/cost/<demo|full>/runs_<UTC>.{json,md}.",
    )
    parser.add_argument(
        "--last-n", type=int, default=5,
        help="With --by-run: restrict the detailed rows and summary statistics to the "
        "most recent N full_run_ids (default: 5). Pass 0 for every run found.",
    )
    parser.add_argument(
        "--since", default=None,
        help="With --by-run: exclude any full_run_id that started before this timestamp "
        "('YYYY-MM-DD HH:MM:SS[.ffffff]' or ISO 8601; naive values are treated as UTC). "
        "Applied before --last-n, so you get the last N runs *after* this cutoff -- e.g. "
        "to exclude runs affected by a bug that has since been fixed.",
    )
    args = parser.parse_args()
    if args.dataset:
        os.environ["MIMIC_DATASET"] = args.dataset

    settings = load_aws_settings()

    if args.by_run:
        pg_settings = load_settings()
        pg_rows = _pg_process_log_rows(pg_settings)
        if not pg_rows:
            print("No full_run_id rows found -- run `mimic-load glue run-all` (or a single glue run-* step) at least once first.")
            return

        cloudwatch = boto3.client("cloudwatch", region_name=settings.region)
        logs_c = boto3.client("logs", region_name=settings.region)
        all_runs = collect_runs(pg_rows, cloudwatch=cloudwatch, logs_client=logs_c)
        since_dt = _parse_ts(args.since) if args.since else None
        eligible = all_runs
        if since_dt is not None:
            eligible = [r for r in all_runs if (_parse_ts(r["started_at"]) or since_dt) >= since_dt]
            excluded = len(all_runs) - len(eligible)
            print(f"[by-run] --since {args.since} -> excluding {excluded} run(s) started before it "
                  f"({len(eligible)} eligible)")
        runs = eligible[-args.last_n:] if args.last_n else eligible
        print(f"[by-run] {len(all_runs)} total run(s) found; reporting the last {len(runs)}"
              f"{' since ' + args.since if since_dt is not None else ''}.")
        print("[by-run] current resource footprint (Aurora, Glue, S3, CloudWatch, IAM) ...")
        footprint = collect_current_footprint(
            boto3.client("rds", region_name=settings.region), cloudwatch,
            boto3.client("glue", region_name=settings.region),
            boto3.client("s3", region_name=settings.region),
            boto3.client("logs", region_name=settings.region),
            boto3.client("iam"),
            settings, args.hours,
        )
        report = {
            "generated_utc": datetime.now(timezone.utc).isoformat(),
            "dataset": settings.dataset,
            "since": args.since,
            "n_runs_excluded_before_since": (len(all_runs) - len(eligible)) if since_dt is not None else 0,
            "last_n": args.last_n or len(eligible),
            "n_runs_available": len(all_runs),
            "n_runs_eligible": len(eligible),
            "runs": runs,
            "summary_statistics": compute_run_statistics(runs),
            "current_footprint": footprint,
        }
        mode = dataset_profile(settings.dataset).mode
        out_dir = Path(__file__).resolve().parents[1] / "docs" / "cost" / mode
        out_dir.mkdir(parents=True, exist_ok=True)
        ts = report["generated_utc"][:19].replace(":", "").replace("-", "")
        json_text = json.dumps(report, indent=2, default=str)
        md_text = render_runs_md(report)
        (out_dir / f"runs_{ts}.json").write_text(json_text, encoding="utf-8")
        (out_dir / "latest_runs.json").write_text(json_text, encoding="utf-8")
        (out_dir / f"runs_{ts}.md").write_text(md_text, encoding="utf-8")
        (out_dir / "latest_runs.md").write_text(md_text, encoding="utf-8")

        for run in runs:
            cost = run["glue_cost_usd"]
            cost_str = f"${cost:.4f}" if cost is not None else "cost n/a"
            print(
                f"full_run_id {run['full_run_id']}: {human_seconds(run['wall_clock_seconds'])} wall, "
                f"{run['job_count']} tables ({run['failed_count']} failed), {cost_str}"
            )
        print(f"report: {out_dir / 'latest_runs.md'}")
        return

    rds = boto3.client("rds", region_name=settings.region)
    glue_client = boto3.client("glue", region_name=settings.region)
    s3_client = boto3.client("s3", region_name=settings.region)
    logs_client = boto3.client("logs", region_name=settings.region)
    iam_client = boto3.client("iam")
    cloudwatch = boto3.client("cloudwatch", region_name=settings.region)

    aurora = aurora_report(rds, cloudwatch, settings.aurora_cluster_id, args.hours)
    glue_data = glue_report(glue_client, settings.glue_job_name_prefix)
    s3_data = s3_report(s3_client, settings.s3_bucket)
    logs_data = logs_report(logs_client)
    iam_data = iam_report(iam_client, [settings.iam_role_name, settings.glue_role_name])
    cost = build_cost_estimate(aurora, glue_data, logs_data)

    if args.json:
        print(
            json.dumps(
                build_json_report(aurora, glue_data, s3_data, logs_data, iam_data, cost,
                                  settings.dataset),
                indent=2,
                default=str,
            )
        )
    else:
        print_report(
            aurora, glue_data, s3_data, logs_data, iam_data, cost, detail=args.detail
        )


if __name__ == "__main__":
    main()
