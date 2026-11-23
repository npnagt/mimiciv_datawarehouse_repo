"""Discovery of MIMIC-IV source tables from the extracted demo dataset.

The dataset ships as `<data_dir>/<module>/<table>.csv.gz`, where `module` is
`hosp` or `icu`. Each module maps 1:1 to a Postgres schema of the same name.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

MODULES = ("hosp", "icu")


@dataclass(frozen=True)
class MimicTable:
    schema: str
    name: str
    csv_path: Path


def discover_tables(data_dir: Path, modules: tuple[str, ...] = MODULES) -> list[MimicTable]:
    tables: list[MimicTable] = []
    for module in modules:
        module_dir = data_dir / module
        if not module_dir.is_dir():
            continue
        for csv_path in sorted(module_dir.glob("*.csv.gz")):
            table_name = csv_path.name.removesuffix(".csv.gz")
            tables.append(MimicTable(schema=module, name=table_name, csv_path=csv_path))
    return tables
