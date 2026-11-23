"""Measure the on-disk storage footprint of the Aurora Postgres data warehouse -- a
baseline for comparing against the equivalent lakehouse (Delta/Parquet on S3, queried via
Athena/Glue) storage report in the other repo.

Reports, per table and rolled up by category:
  - Physical:      pg_total_relation_size -- actual bytes on disk (heap + indexes +
                   TOAST), including any dead tuples/free space not yet reclaimed.
                   Analogous to the lakehouse's "Physical" column (all Delta file
                   versions on S3, not just the ones the latest snapshot needs).
  - Live snapshot: bytes belonging to live (visible, non-dead) rows only -- via the
                   pgstattuple extension's exact tuple_len when available, or estimated
                   from pg_stat_user_tables' live/dead tuple ratio otherwise. Analogous
                   to the lakehouse's "Live snapshot" (current Delta version only, no
                   history). Index/TOAST bytes are counted at their full physical size
                   in both columns -- their own bloat is comparatively small for these
                   tables and not worth a second pgstattuple pass per index.

Categories mirror the lakehouse report so the two can be lined up directly:
  - Structured:      staging.stg_* (raw/bronze-equivalent landing tables) + warehouse.
                     dim_/fact_/agg_* (Gold-equivalent tables) -- the only category this
                     warehouse actually holds any of.
  - Infrastructure:  landing.* -- the throwaway merge-scratch schema each Glue job
                     writes to and drops from within one transaction (see
                     glue_jobs/glue_common.py's _write_landing/merge_into_target); should
                     be empty between runs, so a nonzero total here usually means a
                     prior job failed before reaching its DROP TABLE.
  - Unstructured/NLP: always reported as 0 -- this warehouse has no note text / NLP
                     tables at all (that data, and its storage cost, lives only in the
                     lakehouse repo being compared against).

Usage:
    python scripts/measure_warehouse_size.py                # summary only
    python scripts/measure_warehouse_size.py --detail        # + per-table breakdown
    python scripts/measure_warehouse_size.py --json          # machine-readable output
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from sqlalchemy import text

from mimic_datawarehouse.config import DATASET_CHOICES, load_settings
from mimic_datawarehouse.db import make_engine

MEASURED_SCHEMAS = ["staging", "warehouse", "landing"]

TABLES_WITH_PGSTATTUPLE_SQL = """
    SELECT
        n.nspname AS schema_name,
        c.relname AS table_name,
        pg_total_relation_size(c.oid) AS total_bytes,
        pg_relation_size(c.oid) AS heap_bytes,
        pg_indexes_size(c.oid) AS index_bytes,
        COALESCE(s.n_live_tup, 0) AS n_live_tup,
        (pgstattuple(c.oid)).tuple_len AS live_heap_bytes
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    LEFT JOIN pg_stat_user_tables s ON s.relid = c.oid
    WHERE n.nspname = ANY(:schemas) AND c.relkind = 'r'
    ORDER BY n.nspname, c.relname
"""

# Fallback if the pgstattuple extension can't be created (e.g. insufficient privilege) --
# estimates live heap bytes from the live/dead tuple ratio Postgres already tracks via
# autovacuum's stats collector, rather than the exact per-page scan pgstattuple does.
TABLES_WITHOUT_PGSTATTUPLE_SQL = """
    SELECT
        n.nspname AS schema_name,
        c.relname AS table_name,
        pg_total_relation_size(c.oid) AS total_bytes,
        pg_relation_size(c.oid) AS heap_bytes,
        pg_indexes_size(c.oid) AS index_bytes,
        COALESCE(s.n_live_tup, 0) AS n_live_tup,
        COALESCE(s.n_dead_tup, 0) AS n_dead_tup
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    LEFT JOIN pg_stat_user_tables s ON s.relid = c.oid
    WHERE n.nspname = ANY(:schemas) AND c.relkind = 'r'
    ORDER BY n.nspname, c.relname
"""


@dataclass
class TableSize:
    schema: str
    table: str
    category: str
    row_count: int
    total_bytes: int
    live_bytes: int


def categorize(schema: str) -> str:
    if schema in ("staging", "warehouse"):
        return "structured"
    if schema == "landing":
        return "infrastructure"
    return "other"


def pretty_bytes(n: int) -> str:
    for unit, size in (("GiB", 1024**3), ("MiB", 1024**2), ("KiB", 1024)):
        if n >= size:
            return f"{n / size:.2f} {unit}"
    return f"{n} B"


def collect_table_sizes(conn) -> tuple[list[TableSize], bool]:
    """Returns (per-table sizes, whether pgstattuple's exact live-tuple accounting was
    used). Falls back to the live/dead-ratio estimate if the extension can't be created
    -- e.g. a database user without CREATE privilege on extensions."""
    used_pgstattuple = True
    try:
        conn.execute(text("CREATE EXTENSION IF NOT EXISTS pgstattuple"))
        rows = conn.execute(
            text(TABLES_WITH_PGSTATTUPLE_SQL), {"schemas": MEASURED_SCHEMAS}
        ).fetchall()
    except Exception:  # noqa: BLE001 -- deliberately broad: any failure here (missing
        # privilege, extension not allowed on this host, etc.) should fall back to the
        # estimate below rather than aborting the whole report over an optional extra.
        conn.rollback()
        used_pgstattuple = False
        rows = conn.execute(
            text(TABLES_WITHOUT_PGSTATTUPLE_SQL), {"schemas": MEASURED_SCHEMAS}
        ).fetchall()

    sizes = []
    for row in rows:
        m = row._mapping
        if used_pgstattuple:
            live_heap_bytes = m["live_heap_bytes"] or 0
        else:
            live_ratio = (
                m["n_live_tup"] / (m["n_live_tup"] + m["n_dead_tup"])
                if (m["n_live_tup"] + m["n_dead_tup"])
                else 1.0
            )
            live_heap_bytes = round(m["heap_bytes"] * live_ratio)
        # Index + TOAST bytes are counted at full physical size in both Physical and
        # Live -- see module docstring.
        non_heap_bytes = m["index_bytes"] + (
            m["total_bytes"] - m["heap_bytes"] - m["index_bytes"]
        )
        sizes.append(
            TableSize(
                schema=m["schema_name"],
                table=m["table_name"],
                category=categorize(m["schema_name"]),
                row_count=m["n_live_tup"],
                total_bytes=m["total_bytes"],
                live_bytes=live_heap_bytes + non_heap_bytes,
            )
        )
    return sizes, used_pgstattuple


def print_report(
    sizes: list[TableSize], db_size_bytes: int, used_pgstattuple: bool, detail: bool
) -> None:
    total_physical = (
        sum(t.total_bytes for t in sizes) or 1
    )  # avoid div-by-zero if empty
    structured = [t for t in sizes if t.category == "structured"]
    infrastructure = [t for t in sizes if t.category == "infrastructure"]

    def category_row(label: str, tables: list[TableSize], note: str) -> None:
        physical = sum(t.total_bytes for t in tables)
        live = sum(t.live_bytes for t in tables)
        share = 100.0 * physical / total_physical
        print(
            f"{label:<32} {pretty_bytes(physical):>12} {pretty_bytes(live):>15} {share:>7.1f}%   {note}"
        )

    print("=" * 100)
    print("Aurora data warehouse storage report (baseline for lakehouse comparison)")
    print("=" * 100)
    print(
        f"{'Category':<32} {'Physical':>12} {'Live snapshot':>15} {'Share':>8}   Aurora content"
    )
    print("-" * 100)
    category_row(
        "Structured",
        structured,
        f"staging.stg_* + warehouse.dim_/fact_/agg_* ({len(structured)} tables)",
    )
    category_row(
        "Infrastructure",
        infrastructure,
        f"landing.* ETL merge scratch ({len(infrastructure)} tables; expect ~0 between runs)",
    )
    print(
        f"{'Unstructured / NLP':<32} {pretty_bytes(0):>12} {pretty_bytes(0):>15} {0.0:>7.1f}%   NO -- not stored in Aurora"
    )
    print("-" * 100)
    print(f"{'TOTAL (measured tables)':<32} {pretty_bytes(total_physical):>12}")
    print(
        f"{'Aurora pg_database_size()':<32} {pretty_bytes(db_size_bytes):>12}  (includes catalogs/system overhead not itemized above)"
    )
    print()
    print(
        f"Live-snapshot method: {'pgstattuple (exact)' if used_pgstattuple else 'live/dead tuple ratio (estimated)'}"
    )

    if detail:
        print()
        print("-" * 100)
        print(f"{'schema.table':<55} {'rows':>10} {'physical':>12} {'live':>12}")
        print("-" * 100)
        for t in sorted(sizes, key=lambda t: t.total_bytes, reverse=True):
            print(
                f"{t.schema + '.' + t.table:<55} {t.row_count:>10,} {pretty_bytes(t.total_bytes):>12} {pretty_bytes(t.live_bytes):>12}"
            )


def build_json_report(
    sizes: list[TableSize], db_size_bytes: int, used_pgstattuple: bool,
    dataset: str = "demodataset", database: str | None = None,
) -> dict:
    total_physical = sum(t.total_bytes for t in sizes)
    structured = [t for t in sizes if t.category == "structured"]
    infrastructure = [t for t in sizes if t.category == "infrastructure"]
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "dataset": dataset,
        "database": database,
        "live_snapshot_method": (
            "pgstattuple" if used_pgstattuple else "live_dead_ratio_estimate"
        ),
        "database_total_bytes": db_size_bytes,
        "measured_total_bytes": total_physical,
        "categories": {
            "structured": {
                "physical_bytes": sum(t.total_bytes for t in structured),
                "live_bytes": sum(t.live_bytes for t in structured),
                "table_count": len(structured),
                "in_aurora": True,
            },
            "infrastructure": {
                "physical_bytes": sum(t.total_bytes for t in infrastructure),
                "live_bytes": sum(t.live_bytes for t in infrastructure),
                "table_count": len(infrastructure),
                "in_aurora": True,
            },
            "unstructured_nlp": {
                "physical_bytes": 0,
                "live_bytes": 0,
                "table_count": 0,
                "in_aurora": False,
            },
        },
        "tables": [asdict(t) for t in sizes],
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--detail", action="store_true", help="Also print a per-table breakdown."
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print a machine-readable JSON report instead of text.",
    )
    parser.add_argument(
        "--dataset", choices=DATASET_CHOICES, default=None,
        help="Dataset whose warehouse DB to measure (default: demodataset, or MIMIC_DATASET).",
    )
    args = parser.parse_args()
    if args.dataset:
        os.environ["MIMIC_DATASET"] = args.dataset

    settings = load_settings()
    engine = make_engine(settings)
    with engine.begin() as conn:
        sizes, used_pgstattuple = collect_table_sizes(conn)
        db_size_bytes = conn.execute(
            text("SELECT pg_database_size(current_database())")
        ).scalar()

    if args.json:
        print(
            json.dumps(
                build_json_report(sizes, db_size_bytes, used_pgstattuple,
                                  settings.dataset, settings.rds_database), indent=2
            )
        )
    else:
        print_report(sizes, db_size_bytes, used_pgstattuple, detail=args.detail)


if __name__ == "__main__":
    main()
