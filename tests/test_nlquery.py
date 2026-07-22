import json
from types import SimpleNamespace

import polars as pl
import pytest

from src.database import MedallionVault
from src.nlquery import QueryAgent, QueryAgentError


@pytest.fixture
def vault(tmp_path):
    v = MedallionVault(str(tmp_path / "test.duckdb"))
    clean = pl.DataFrame({"category": ["A", "A", "B"], "amount": [10.0, 20.0, 5.0]})
    v.load_to_bronze(clean, "orders")
    v.promote_to_silver("orders", "orders_clean", clean)
    v.promote_to_gold(
        "orders_by_category",
        "SELECT category, SUM(amount) AS total FROM silver.orders_clean GROUP BY category",
        source_table="orders_clean",
    )
    yield v
    v.close()


def _fake_anthropic_client(response_json: dict):
    """Builds a stand-in for anthropic.Anthropic() that returns response_json."""
    text_block = SimpleNamespace(text=json.dumps(response_json))
    response = SimpleNamespace(content=[text_block])

    class _Messages:
        def create(self, **kwargs):
            return response

    return SimpleNamespace(messages=_Messages())


def test_query_agent_requires_api_key(vault):
    agent = QueryAgent(vault, api_key=None)
    with pytest.raises(QueryAgentError):
        agent.ask("How many categories are there?")


def test_query_agent_executes_generated_select(vault, monkeypatch):
    agent = QueryAgent(vault, api_key="fake-key")
    monkeypatch.setattr(
        agent, "_generate_sql",
        lambda question: {
            "sql": "SELECT * FROM gold.orders_by_category",
            "explanation": "Returns totals per category.",
        },
    )
    result = agent.ask("What's the total per category?")
    assert result["sql"].lower().startswith("select")
    assert result["result"].height == 2


def test_query_agent_rejects_non_select_sql(vault, monkeypatch):
    agent = QueryAgent(vault, api_key="fake-key")
    monkeypatch.setattr(
        agent, "_generate_sql",
        lambda question: {"sql": "DROP TABLE gold.orders_by_category", "explanation": "n/a"},
    )
    with pytest.raises(QueryAgentError):
        agent.ask("Delete everything")


def test_query_agent_rejects_empty_sql_with_explanation(vault, monkeypatch):
    agent = QueryAgent(vault, api_key="fake-key")
    monkeypatch.setattr(
        agent, "_generate_sql",
        lambda question: {"sql": "", "explanation": "No matching table for that question."},
    )
    with pytest.raises(QueryAgentError, match="No matching table"):
        agent.ask("What's the weather like?")


def test_validate_sql_blocks_stacked_statements():
    with pytest.raises(QueryAgentError):
        QueryAgent._validate_sql("SELECT * FROM gold.orders_by_category; DROP TABLE gold.orders_by_category")
