import polars as pl
import pytest

from src.database import MedallionVault


@pytest.fixture
def vault(tmp_path):
    v = MedallionVault(str(tmp_path / "test.duckdb"))
    yield v
    v.close()


def test_bronze_load_and_read(vault):
    df = pl.DataFrame({"a": [1, 2, 3]})
    vault.load_to_bronze(df, "sample")
    result = vault.read_table("bronze", "sample")
    assert result.height == 3
    assert "sample" in vault.list_tables("bronze")


def test_silver_promotion_and_lineage(vault):
    raw = pl.DataFrame({"a": [1, 2, 3]})
    clean = pl.DataFrame({"a": [1, 2]})
    vault.load_to_bronze(raw, "sample")
    vault.promote_to_silver("sample", "sample_clean", clean)

    silver = vault.read_table("silver", "sample_clean")
    assert silver.height == 2

    lineage = vault.lineage_for("sample")
    assert lineage.height >= 2  # bronze load + silver promotion


def test_gold_aggregation(vault):
    clean = pl.DataFrame({"category": ["A", "A", "B"], "amount": [10.0, 20.0, 5.0]})
    vault.load_to_bronze(clean, "orders")
    vault.promote_to_silver("orders", "orders_clean", clean)
    gold = vault.promote_to_gold(
        "orders_by_category",
        "SELECT category, SUM(amount) AS total FROM silver.orders_clean GROUP BY category",
        source_table="orders_clean",
    )
    assert gold.height == 2


def test_record_and_read_quality_history(vault):
    vault.record_run("run-1", "orders", "complete", 100, 92.5, 3, 2)
    history = vault.quality_history("orders")
    assert history.height == 1
    assert history["overall_score"][0] == 92.5
