import polars as pl
import pytest

from src.connectors import ConnectorError, CSVSource, get_source
from src.connectors.mysql_source import MySQLSource
from src.connectors.postgres_source import PostgresSource
from src.connectors.s3_source import S3Source


def test_get_source_returns_csv_by_default(tmp_path):
    csv_path = tmp_path / "orders.csv"
    csv_path.write_text("id,amount\n1,10.0\n2,20.0\n")

    source = get_source("csv", path=str(csv_path))
    assert isinstance(source, CSVSource)
    df = source.extract()
    assert df.height == 2
    assert source.describe() == {"source_type": "csv", "path": str(csv_path)}


def test_csv_source_missing_file_raises_connector_error(tmp_path):
    source = CSVSource(str(tmp_path / "does_not_exist.csv"))
    with pytest.raises(ConnectorError):
        source.extract()


def test_get_source_rejects_unknown_type():
    with pytest.raises(ValueError):
        get_source("carrier_pigeon")


def test_postgres_source_requires_table_or_query():
    with pytest.raises(ValueError):
        PostgresSource(host="db", database="orders", user="ro", password="x")


def test_mysql_source_requires_table_or_query():
    with pytest.raises(ValueError):
        MySQLSource(host="db", database="orders", user="ro", password="x")


def test_s3_source_rejects_bad_format():
    with pytest.raises(ValueError):
        S3Source(bucket="b", key="k", file_format="xml")


def test_postgres_source_describe_never_leaks_password():
    source = PostgresSource(host="db", database="orders", user="ro",
                             password="super-secret", table="orders")
    described = source.describe()
    assert "super-secret" not in str(described)


def test_registry_dispatches_to_correct_connector():
    csv_src = get_source("csv", path="data/raw/orders.csv")
    pg_src = get_source("postgres", host="db", database="orders", user="ro",
                         password="x", table="orders")
    mysql_src = get_source("mysql", host="db", database="orders", user="ro",
                            password="x", table="orders")
    s3_src = get_source("s3", bucket="b", key="k")

    assert csv_src.source_type == "csv"
    assert pg_src.source_type == "postgres"
    assert mysql_src.source_type == "mysql"
    assert s3_src.source_type == "s3"
