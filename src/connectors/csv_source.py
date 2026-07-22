"""CSVSource — the original extraction path, now as a connector.

This wraps the exact behavior ``foundry_flows.extract()`` always had
(``pl.read_csv(path, try_parse_dates=True)``), so pointing the pipeline
at ``CSVSource`` instead of a bare path is a no-op for existing users.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import polars as pl

from .base import ConnectorError, DataSource


class CSVSource(DataSource):
    source_type = "csv"

    def __init__(self, path: str):
        self.path = path

    def extract(self) -> pl.DataFrame:
        if not Path(self.path).exists():
            raise ConnectorError(
                f"No raw data found at {self.path}. "
                "Run `python scripts/generate_sample_data.py` first."
            )
        return pl.read_csv(self.path, try_parse_dates=True)

    def describe(self) -> dict[str, Any]:
        return {"source_type": self.source_type, "path": self.path}
