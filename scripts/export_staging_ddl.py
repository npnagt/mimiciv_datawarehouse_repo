"""Regenerate sql/staging_tables.sql from mimic_datawarehouse.staging.STAGING_TABLE_COLUMNS.

Run this after editing STAGING_TABLE_COLUMNS so the exported file stays in sync with
the DDL `mimic-load create-staging` actually executes.

Usage:
    python scripts/export_staging_ddl.py
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from mimic_datawarehouse.staging import render_ddl  # noqa: E402

OUTPUT_PATH = Path(__file__).resolve().parents[1] / "sql" / "staging_tables.sql"


def main() -> None:
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(render_ddl(), encoding="utf-8", newline="\n")
    print(f"Wrote {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
