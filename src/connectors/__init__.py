"""
Multi-source connectors for Agentic Aegis
===========================================

Everything Scout/Sentinel/Healer/Oracle and the MedallionVault do is
independent of *where* rows came from. This package is the boundary
that turns "a CSV, a Postgres table, a MySQL table, or an S3 object"
into a single Polars DataFrame the rest of the pipeline already knows
how to work with — nothing downstream changes.

    from src.connectors import get_source
    df = get_source("csv", path="data/raw/orders.csv").extract()
    df = get_source("postgres", host="db", database="orders",
                     user="ro", password="...", table="orders").extract()
"""

from .base import ConnectorError, DataSource
from .csv_source import CSVSource
from .registry import get_source

__all__ = ["DataSource", "ConnectorError", "CSVSource", "get_source"]
