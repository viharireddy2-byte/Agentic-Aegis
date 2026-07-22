"""
DataSource — common interface for every Aegis connector
=========================================================

Every connector (CSV, Postgres, MySQL, S3, ...) implements the same
tiny contract: ``extract()`` returns a Polars DataFrame and ``describe()``
returns a JSON-safe dict identifying where the data came from. This is
what lets ``foundry_flows.py`` stay agnostic about *where* rows came from
— Scout/Sentinel/Healer/Oracle never need to know.

Adding a new source later (an API, a message queue snapshot, another
warehouse) means writing one small class here, not touching the
pipeline or the agents.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

import polars as pl


class ConnectorError(RuntimeError):
    """Raised when a connector can't reach or read its source.

    Kept distinct from generic exceptions so orchestration code can
    catch connector failures specifically (e.g. to retry or alert)
    without swallowing unrelated bugs.
    """


class DataSource(ABC):
    """Abstract base class for all Aegis data sources."""

    #: short machine-readable identifier, e.g. "csv", "postgres"
    source_type: str = "base"

    @abstractmethod
    def extract(self) -> pl.DataFrame:
        """Pull the full dataset and return it as a Polars DataFrame."""

    def describe(self) -> dict[str, Any]:
        """JSON-safe metadata about this source, for lineage/logging.

        Subclasses should override to add non-sensitive details (host,
        bucket, table name) — never credentials.
        """
        return {"source_type": self.source_type}
