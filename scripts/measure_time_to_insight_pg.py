"""Time-to-insight test harness for the Aurora Postgres data warehouse.

NOT part of the formal P6 test. P6 (dissertation Ch.1 Table 2) is a standalone
threshold test of the lakehouse's own unified BI layer ("Unified abstraction layer
supports rendering of results inside Nielsen's 'sustained attention' band (1-10 s),
rather than the >10-second 'attention lost' band") with NO baseline comparison, so
this warehouse-side measurement is not required and is not run by
run_measurement_suite.py or read by compare_environments.py. Kept only as an optional,
supplementary point of context for whoever wants to see how a decoupled warehouse's own
BI layer performs, run manually and interpreted on its own -- not as evidence for or
against P6.

Same 19-query intent as the lakehouse repo's measure_time_to_insight.py, translated to
the `warehouse` schema and timed against Aurora:

    time_to_insight = client wall-clock (planning + execution + fetch + network RTT)
                    + --render-budget-ms   (fixed BI render allowance, default 500)

Server-side planning / execution ms are also captured via EXPLAIN (ANALYZE, FORMAT JSON)
for the breakdown. Each query runs --repeats times; the first is discarded and the
median / p90 / max of the rest is bucketed into a Nielsen band (A <1s / B 1-10s / C >=10s).

Three tiers have NO warehouse equivalent and are reported as unsupported:
  - T4 wide OBT lookup  -> the warehouse has no obt_* tables; the analyst must rebuild
                           the features with a live multi-way join every time (measured
                           here as T4j).
  - T5 structured x NLP -> no fact_note_nlp* table in the warehouse.
  - T6 cross-modal JSON -> no fact_radiology_note_nlp* table in the warehouse.

    python scripts/measure_time_to_insight_pg.py                 # 5 repeats
    python scripts/measure_time_to_insight_pg.py --repeats 8 --only T2,T7

Writes docs/tti/tti_pg_<UTC>.{json,md} + latest_pg.{json,md}. JSON shape matches the
lakehouse harness's report structure for easy side-by-side reading, even though nothing
currently merges the two automatically.
"""
from __future__ import annotations

import argparse
import json
import os
import statistics
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from mimic_datawarehouse.config import DATASET_CHOICES, load_settings  # noqa: E402
from mimic_datawarehouse.db import make_engine  # noqa: E402
from sqlalchemy import text  # noqa: E402

BAND_FLOW = 1000
BAND_ATTENTION = 10000
W = "warehouse"

QUERIES = [
    ("floor", "T0", "Fixed floor (SELECT 1)", "SELECT 1", "runnable"),

    ("T1a", "T1", "Admission volume trend by month",
     f"SELECT year_month, sum(admit_count) admits FROM {W}.agg_admission_monthly GROUP BY 1 ORDER BY 1", "runnable"),
    ("T1b", "T1", "Case mix by admission type",
     f"SELECT admission_type, count(*) n FROM {W}.fact_admission GROUP BY 1 ORDER BY 2 DESC", "runnable"),

    ("T2a", "T2", "30-day readmission rate by DRG severity (join)",
     f"SELECT d.drg_severity, count(*) n, round(avg(a.is_readmission_flag::int)::numeric, 3) readmit "
     f"FROM {W}.fact_admission a JOIN {W}.fact_drg_assignment d USING (hadm_id) "
     f"WHERE d.drg_severity IS NOT NULL GROUP BY 1 ORDER BY 1", "runnable"),
    ("T2b", "T2", "Abnormal-lab burden per admission type (join)",
     f"SELECT a.admission_type, count(*) labs, round(avg(l.is_abnormal_flag::int)::numeric, 3) abn_rate "
     f"FROM {W}.fact_lab_result l JOIN {W}.fact_admission a USING (hadm_id) GROUP BY 1 ORDER BY 3 DESC", "runnable"),

    ("T3a", "T3", "Abnormal vs normal lab counts (108k rows)",
     f"SELECT is_abnormal_flag, count(*) n FROM {W}.fact_lab_result GROUP BY 1", "runnable"),
    ("T3b", "T3", "Medication-administration volume + reach (36k rows)",
     f"SELECT count(*) admins, count(DISTINCT hadm_id) admissions FROM {W}.fact_medication_administration", "runnable"),
    ("T3c", "T3", "Positive blood-culture rate by month",
     f"SELECT chart_date_key / 100 AS ym, count(*) tests, "
     f"round(avg(is_positive_culture_flag::int)::numeric, 3) pos_rate "
     f"FROM {W}.fact_microbiology_result GROUP BY 1 ORDER BY 1", "runnable"),

    ("T4j", "T4", "Admission features -- rebuilt inline (no obt_* table in the warehouse)",
     f"SELECT a.admission_type, count(*) admits, round(avg(a.hospital_los_hours)::numeric, 1) mean_los_hr, "
     f"round(avg(dx.n)::numeric, 1) mean_dx, round(avg(lab.abn)::numeric, 3) abn_lab_rate "
     f"FROM {W}.fact_admission a "
     f"LEFT JOIN (SELECT hadm_id, count(*) n FROM {W}.fact_diagnosis GROUP BY 1) dx USING (hadm_id) "
     f"LEFT JOIN (SELECT hadm_id, avg(is_abnormal_flag::int) abn FROM {W}.fact_lab_result GROUP BY 1) lab USING (hadm_id) "
     f"LEFT JOIN {W}.fact_drg_assignment d USING (hadm_id) "
     f"GROUP BY 1 ORDER BY 2 DESC", "runnable"),

    ("T5a", "T5", "Readmission by note-derived tobacco_use (structured x NLP)", None, "unsupported"),
    ("T5b", "T5", "LOS by note-derived obesity_level (structured x NLP)", None, "unsupported"),
    ("T5c", "T5", "SDOH burden cross-tab (structured x NLP)", None, "unsupported"),
    ("T6a", "T6", "Radiology-reported disorders ranked (cross-modal JSON)", None, "unsupported"),
    ("T6b", "T6", "Imaging modality mix per admission (cross-modal)", None, "unsupported"),
    ("T6c", "T6", "Radiology ICD codes vs coded diagnoses (cross-modal)", None, "unsupported"),

    ("T7a", "T7", "Chart-observation volume + reach (669k rows)",
     f"SELECT count(*) rows, count(DISTINCT hadm_id) admissions, count(DISTINCT subject_id) patients "
     f"FROM {W}.fact_chart_observation", "runnable"),
    ("T7b", "T7", "Charted abnormality rate by month (669k rows + group)",
     f"SELECT chart_date_key / 100 AS ym, count(*) obs, "
     f"round(avg(is_abnormal_flag::int)::numeric, 3) abn_rate "
     f"FROM {W}.fact_chart_observation GROUP BY 1 ORDER BY 1", "runnable"),
    ("T7c", "T7", "Numeric vitals per ICU stay (669k rows)",
     f"SELECT stay_id, count(*) numeric_obs FROM {W}.fact_chart_observation "
     f"WHERE value_num IS NOT NULL AND stay_id IS NOT NULL GROUP BY 1 ORDER BY 2 DESC LIMIT 25", "runnable"),
]

TIER_LABEL = {
    "T0": "floor", "T1": "structured, single table", "T2": "structured join",
    "T3": "event-fact aggregation", "T4": "wide features (no OBT -> inline join)",
    "T5": "structured x NLP", "T6": "cross-modal JSON", "T7": "big event-fact scan",
}


def band(ms):
    return "A" if ms < BAND_FLOW else ("B" if ms < BAND_ATTENTION else "C")


def pctl(vals, p):
    v = sorted(vals)
    return v[min(len(v) - 1, round(p * (len(v) - 1)))]


def run_once(conn, sql):
    t0 = time.time()
    rows = conn.execute(text(sql)).fetchall()
    client_ms = (time.time() - t0) * 1000
    plan = conn.execute(text(f"EXPLAIN (ANALYZE, FORMAT JSON) {sql}")).scalar()
    if isinstance(plan, str):
        plan = json.loads(plan)
    p = plan[0]
    return {
        "client_ms": client_ms,
        "planning_ms": p.get("Planning Time", 0.0),
        "execution_ms": p.get("Execution Time", 0.0),
        "rows": len(rows),
    }


def measure(conn, sql, repeats, render_ms):
    runs = []
    for _ in range(repeats):
        r = run_once(conn, sql)
        r["tti_ms"] = r["client_ms"] + render_ms
        runs.append(r)
    scored = runs[1:] if len(runs) > 1 else runs
    tti = [x["tti_ms"] for x in scored]
    return {
        "scored_runs": len(scored),
        "tti_median_ms": int(statistics.median(tti)),
        "tti_p90_ms": int(pctl(tti, 0.9)),
        "tti_max_ms": int(max(tti)),
        "client_median_ms": int(statistics.median(x["client_ms"] for x in scored)),
        "planning_median_ms": round(statistics.median(x["planning_ms"] for x in scored), 1),
        "execution_median_ms": round(statistics.median(x["execution_ms"] for x in scored), 1),
        "rows": scored[-1]["rows"],
        "band": band(int(statistics.median(tti))),
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--repeats", type=int, default=5)
    ap.add_argument("--render-budget-ms", type=int, default=500)
    ap.add_argument("--only", default=None, help="comma-separated tier or query ids")
    ap.add_argument("--out-dir", default=None,
                    help="output dir (default: docs/tti/<demo|full> for the --dataset)")
    ap.add_argument("--label", default=None)
    ap.add_argument("--dataset", choices=DATASET_CHOICES, default=None,
                    help="Dataset whose warehouse DB to measure (default: demodataset, or MIMIC_DATASET).")
    args = ap.parse_args()
    if args.dataset:
        os.environ["MIMIC_DATASET"] = args.dataset
    settings = load_settings()
    if args.out_dir is None:
        mode = "full" if settings.dataset == "fulldataset" else "demo"
        args.out_dir = str(Path(__file__).resolve().parents[1] / "docs" / "tti" / mode)

    want = {x.strip() for x in args.only.split(",")} if args.only else None
    battery = [q for q in QUERIES if not want or q[0] in want or q[1] in want or q[0] == "floor"]

    eng = make_engine(settings)
    print(f"time-to-insight (Postgres) :: warehouse schema :: {len(battery)} queries x {args.repeats} repeats "
          f":: render budget {args.render_budget_ms} ms")
    print(f"Nielsen :: A < {BAND_FLOW}  |  B {BAND_FLOW}-{BAND_ATTENTION}  |  C >= {BAND_ATTENTION}\n")

    results = []
    resume_ms = None
    with eng.connect() as conn:
        # first touch -- captures any Serverless v2 scale-from-zero resume latency
        t0 = time.time()
        conn.execute(text("SELECT 1")).fetchall()
        resume_ms = int((time.time() - t0) * 1000)
        print(f"first-touch latency (incl. any Serverless v2 resume): {resume_ms} ms\n")
        for qid, tier, label, sql, status in battery:
            if status == "unsupported":
                print(f"  {qid:6} {tier}  --  NO WAREHOUSE PATH ({label})")
                results.append({"id": qid, "tier": tier, "label": label, "status": "unsupported",
                                "reason": "no fact_note_nlp* / fact_radiology_note_nlp* table in the warehouse schema"})
                continue
            try:
                m = measure(conn, sql, args.repeats, args.render_budget_ms)
            except Exception as e:  # noqa: BLE001
                print(f"  {qid:6} {tier}  FAILED  {e}")
                results.append({"id": qid, "tier": tier, "label": label, "status": "error", "error": str(e)})
                continue
            m.update(id=qid, tier=tier, label=label, sql=sql, status="runnable")
            results.append(m)
            print(f"  {qid:6} {tier}  {m['band']}  tti med {m['tti_median_ms']:>5} ms  (p90 {m['tti_p90_ms']:>5})  "
                  f"plan {m['planning_median_ms']:>5}  exec {m['execution_median_ms']:>7}  {m['rows']} rows")

    ins = [r for r in results if r.get("status") == "runnable" and r["id"] != "floor"]
    unsup = [r for r in results if r.get("status") == "unsupported"]
    counts = {b: sum(1 for r in ins if r["band"] == b) for b in ("A", "B", "C")}
    floor = next((r for r in results if r["id"] == "floor"), {})
    rollup = {
        "queries_scored": len(ins),
        "unsupported": len(unsup),
        "band_A_flow": counts["A"], "band_B_attention": counts["B"], "band_C_lost": counts["C"],
        "pct_within_attention_of_runnable": round(100 * (counts["A"] + counts["B"]) / len(ins), 1) if ins else None,
        "pct_within_flow_of_runnable": round(100 * counts["A"] / len(ins), 1) if ins else None,
        "pct_answerable": round(100 * len(ins) / (len(ins) + len(unsup)), 1) if (ins or unsup) else None,
        "overall_p90_ms": pctl([r["tti_median_ms"] for r in ins], 0.9) if ins else None,
        "floor_ms": floor.get("tti_median_ms"),
        "first_touch_ms": resume_ms,
    }

    report = {
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "dataset": settings.dataset, "database": settings.rds_database,
        "engine": "aurora-postgresql", "schema": W, "label": args.label,
        "params": {"repeats": args.repeats, "render_budget_ms": args.render_budget_ms,
                   "band_flow_ms": BAND_FLOW, "band_attention_ms": BAND_ATTENTION},
        "rollup": rollup, "queries": results,
    }
    out = Path(args.out_dir)
    out.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    (out / f"tti_pg_{ts}.json").write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
    (out / "latest_pg.json").write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
    (out / f"tti_pg_{ts}.md").write_text(render_md(report), encoding="utf-8")
    (out / "latest_pg.md").write_text(render_md(report), encoding="utf-8")

    print("\n" + "=" * 60)
    print(f"runnable: {rollup['pct_within_attention_of_runnable']}% within attention band, "
          f"{rollup['pct_within_flow_of_runnable']}% within flow; p90 {rollup['overall_p90_ms']} ms")
    print(f"answerable at all: {rollup['pct_answerable']}%  ({rollup['unsupported']} tiers have no warehouse path)")
    print(f"first-touch latency: {rollup['first_touch_ms']} ms   floor: {rollup['floor_ms']} ms")
    print(f"report: {out / 'latest_pg.md'}")
    return 0


def render_md(r):
    w = []
    P = w.append
    ro = r["rollup"]
    P("# Time-to-insight (P6) -- Aurora Postgres warehouse")
    P("")
    P(f"- **Generated:** {r['generated_utc']}" + (f"  ·  label `{r['label']}`" if r["label"] else ""))
    P(f"- **Engine:** {r['engine']}, schema `{r['schema']}`")
    P(f"- **time_to_insight** = client wall-clock (plan + exec + fetch + RTT) + "
      f"{r['params']['render_budget_ms']} ms render allowance")
    P(f"- **First-touch latency** (incl. any Serverless v2 scale-from-zero): {ro['first_touch_ms']} ms")
    P("")
    P("## Result")
    P("")
    P("| Metric | Value |")
    P("|---|--:|")
    P(f"| Runnable queries | {ro['queries_scored']} |")
    P(f"| Tiers with **no warehouse path** (T5 NLP, T6 cross-modal) | {ro['unsupported']} |")
    P(f"| % of the 19-query battery answerable at all | **{ro['pct_answerable']}%** |")
    P(f"| Runnable -- band A (< 1 s) | {ro['band_A_flow']} |")
    P(f"| Runnable -- band B (1-10 s) | {ro['band_B_attention']} |")
    P(f"| Runnable -- band C (>= 10 s) | {ro['band_C_lost']} |")
    P(f"| Runnable within attention band | **{ro['pct_within_attention_of_runnable']}%** |")
    P(f"| Runnable within flow band | {ro['pct_within_flow_of_runnable']}% |")
    P(f"| Overall p90 (runnable) | {ro['overall_p90_ms']} ms |")
    P(f"| Floor (SELECT 1) | {ro['floor_ms']} ms |")
    P("")
    P("## Per query")
    P("")
    P("| id | tier | insight | band | tti median | p90 | planning | execution | rows |")
    P("|---|---|---|:--:|--:|--:|--:|--:|--:|")
    for m in r["queries"]:
        if m.get("status") == "unsupported":
            P(f"| `{m['id']}` | {m['tier']} | {m['label']} | — | — | — | — | — | no path |")
            continue
        if m.get("status") == "error":
            P(f"| `{m['id']}` | {m['tier']} | {m['label']} | ERR | | | | | |")
            continue
        P(f"| `{m['id']}` | {m['tier']} ({TIER_LABEL.get(m['tier'],'')}) | {m['label']} | **{m['band']}** | "
          f"{m['tti_median_ms']} ms | {m['tti_p90_ms']} | {m['planning_median_ms']} | "
          f"{m['execution_median_ms']} | {m['rows']} |")
    P("")
    P("## Notes")
    P("")
    P("- Buffer-cache warmth: the first run of each query is discarded, so these are warm-cache "
      "times. A genuinely cold warehouse (just resumed, cache empty) adds the first-touch latency "
      "above plus per-query disk reads.")
    P("- The warehouse has only primary-key indexes on the fact tables; the aggregations here are "
      "sequential scans + hash aggregate.")
    P("- T4 has no `obt_*` table in the warehouse -- T4j measures the live multi-way join the "
      "analyst must run instead of one OBT scan.")
    P("- T5 / T6 return no result at any latency: the NLP tables do not exist in this schema and "
      "there is no pathway to add them without building an NLP pipeline first.")
    P("")
    return "\n".join(w) + "\n"


if __name__ == "__main__":
    raise SystemExit(main())
