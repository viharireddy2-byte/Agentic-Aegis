"""PostgresSource — pulls a table or query result from PostgreSQL.

Requires the ``connectors`` extra (``pip install -r requirements-extra.txt``),
which installs SQLAlchemy + psycopg2-binary. The base install stays
untouched: importing ``src.connectors`` never requires Postgres drivers
unless this specific source is used.
"""

from __future__ import annotations

from typing import Any

import polars as pl

from .base import ConnectorError, DataSource

try:
    import sqlalchemy
except ImportError:  # pragma: no cover - optional dependency
    sqlalchemy = None


class PostgresSource(DataSource):
    source_type = "postgres"

    def __init__(self, host: str, database: str, user: str, password: str,
                 port: int = 5432, table: str | None = None, query: str | None = None):
        if not table and not query:
            raise ValueError("PostgresSource needs either `table` or `query`.")
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.table = table
        self.query = query

    def _connection_url(self) -> str:
        return (
            f"postgresql+psycopg2://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.database}"
        )

    def extract(self) -> pl.DataFrame:
        if sqlalchemy is None:
            raise ConnectorError(
                "PostgresSource requires SQLAlchemy + psycopg2-binary. "
                "Install with: pip install -r requirements-extra.txt"
            )
        stmt = self.query or f"SELECT * FROM {self.table}"
        try:
            engine = sqlalchemy.create_engine(self._connection_url())
            with engine.connect() as conn:
                return pl.read_database(query=stmt, connection=conn)
        except Exception as exc:  # noqa: BLE001 - re-raised as a ConnectorError
            raise ConnectorError(f"PostgresSource failed to read from "
                                  f"{self.host}:{self.port}/{self.database}: {exc}") from exc

    def describe(self) -> dict[str, Any]:
        return {
            "source_type": self.source_type,
            "host": self.host,
            "port": self.port,
            "database": self.database,
            "table": self.table,
            "query": bool(self.query),
        }
