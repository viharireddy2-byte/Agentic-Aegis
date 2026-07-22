"""get_source() — builds a DataSource from config, keeping orchestration
code source-agnostic.

``foundry_flows.py`` calls ``get_source()`` instead of instantiating a
connector directly, so switching the pipeline's input from a CSV to
Postgres/MySQL/S3 is a config change (``AEGIS_SOURCE_TYPE`` env var, or
an explicit ``source_type=`` kwarg), not a code change.
"""

from __future__ import annotations

from typing import Any

from .base import DataSource
from .csv_source import CSVSource

_REGISTRY: dict[str, type[DataSource]] = {
    "csv": CSVSource,
}


def _lazy_register() -> None:
    """Registers connectors whose optional deps might not be installed.

    Deferred so importing this module never fails just because, say,
    boto3 isn't installed — the connector itself already degrades
    gracefully (raises ConnectorError on ``extract()``), this just
    keeps it out of the registry-construction path entirely.
    """
    if "postgres" not in _REGISTRY:
        from .postgres_source import PostgresSource
        from .mysql_source import MySQLSource
        from .s3_source import S3Source
        _REGISTRY["postgres"] = PostgresSource
        _REGISTRY["mysql"] = MySQLSource
        _REGISTRY["s3"] = S3Source


def get_source(source_type: str = "csv", **kwargs: Any) -> DataSource:
    """Instantiate a ``DataSource`` by name.

    Example:
        get_source("csv", path="data/raw/orders.csv")
        get_source("postgres", host="db", database="orders", user="ro",
                    password="...", table="orders")
    """
    _lazy_register()
    source_type = source_type.lower()
    if source_type not in _REGISTRY:
        raise ValueError(
            f"Unknown source_type '{source_type}'. Available: {sorted(_REGISTRY)}"
        )
    return _REGISTRY[source_type](**kwargs)
