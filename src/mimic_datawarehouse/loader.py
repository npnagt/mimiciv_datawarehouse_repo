"""Load a single MIMIC-IV CSV.gz table into RDS in chunks."""

from __future__ import annotations

import logging

import pandas as pd
from sqlalchemy import Engine

from .tables import MimicTable

logger = logging.getLogger(__name__)

DEFAULT_CHUNKSIZE = 50_000


def load_table(
    engine: Engine,
    table: MimicTable,
    *,
    chunksize: int = DEFAULT_CHUNKSIZE,
    if_exists: str = "replace",
) -> int:
    """Stream `table.csv_path` into `table.schema`.`table.name`. Returns row count loaded."""
    total_rows = 0
    reader = pd.read_csv(table.csv_path, compression="gzip", chunksize=chunksize, low_memory=False)
    for chunk_num, chunk in enumerate(reader):
        chunk.to_sql(
            table.name,
            engine,
            schema=table.schema,
            if_exists=if_exists if chunk_num == 0 else "append",
            index=False,
        )
        total_rows += len(chunk)
        logger.info("%s.%s: loaded %d rows (running total %d)", table.schema, table.name, len(chunk), total_rows)
    return total_rows
