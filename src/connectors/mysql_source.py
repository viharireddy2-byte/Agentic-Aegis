"""MySQLSource — pulls a table or query result from MySQL/MariaDB.

Same optional-dependency pattern as ``PostgresSource``: requires
SQLAlchemy + PyMySQL from ``requirements-extra.txt``.
"""

from __future__ import annotations

from typing import Any

import polars as pl

from .base import ConnectorError, DataSource

try:
    import sqlalchemy
except ImportError:  # pragma: no cover - optional dependency
    sqlalchemy = None


class MySQLSource(DataSource):
    source_type = "mysql"

    def __init__(self, host: str, database: str, user: str, password: str,
                 port: int = 3306, table: str | None = None, query: str | None = None):
        if not table and not query:
            raise ValueError("MySQLSource needs either `table` or `query`.")
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.table = table
        self.query = query

    def _connection_url(self) -> str:
        return (
            f"mysql+pymysql://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.database}"
        )

    def extract(self) -> pl.DataFrame:
        if sqlalchemy is None:
            raise ConnectorError(
                "MySQLSource requires SQLAlchemy + PyMySQL. "
                "Install with: pip install -r requirements-extra.txt"
            )
        stmt = self.query or f"SELECT * FROM {self.table}"
        try:
            engine = sqlalchemy.create_engine(self._connection_url())
            with engine.connect() as conn:
                return pl.read_database(query=stmt, connection=conn)
        except Exception as exc:  # noqa: BLE001 - re-raised as a ConnectorError
            raise ConnectorError(f"MySQLSource failed to read from "
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
