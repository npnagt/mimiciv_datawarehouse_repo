"""Regenerate sql/warehouse_tables.sql from mimic_datawarehouse.warehouse.WAREHOUSE_TABLES.

Run this after editing WAREHOUSE_TABLES so the exported file stays in sync with the DDL
`mimic-load create-warehouse` actually executes.

Usage:
    python scripts/export_warehouse_ddl.py
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from mimic_datawarehouse.warehouse import render_ddl  # noqa: E402

OUTPUT_PATH = Path(__file__).resolve().parents[1] / "sql" / "warehouse_tables.sql"


def main() -> None:
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(render_ddl(), encoding="utf-8", newline="\n")
    print(f"Wrote {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
