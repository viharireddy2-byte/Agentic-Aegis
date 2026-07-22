"""
QueryAgent — natural-language questions over the medallion warehouse
========================================================================

The four pipeline agents (Scout/Sentinel/Healer/Oracle) are rule-based
and statistical by design — deterministic and auditable, which is the
right call for remediation. QueryAgent is deliberately different: it's
the one place an LLM reasons about the data, translating a plain-English
question into DuckDB SQL against the Silver/Gold schema and explaining
the result.

Safety model:
    * Only ``SELECT`` statements are ever executed — anything else
      returned by the model is rejected before it touches the vault.
    * The model only ever sees table/column *names and types*, never
      row-level data, when building the query.
    * Requires ``ANTHROPIC_API_KEY``; degrades to a clear error (not a
      crash) if it's unset, so the rest of Aegis is unaffected.

    from src.nlquery import QueryAgent
    agent = QueryAgent(vault)
    result = agent.ask("Which category had the highest average order value?")
    print(result["sql"], result["answer"])
"""

from __future__ import annotations

import json
import os
import re
from typing import Any

import polars as pl

from src.database import MedallionVault

try:
    import anthropic
except ImportError:  # pragma: no cover - optional dependency
    anthropic = None

_FORBIDDEN = re.compile(
    r"\b(insert|update|delete|drop|alter|create|truncate|attach|copy|pragma|call)\b",
    re.IGNORECASE,
)

_SYSTEM_PROMPT = """You are a SQL generator for a DuckDB warehouse with Bronze/Silver/Gold \
medallion schemas. Given a question and the schema below, respond with ONLY a JSON object: \
{"sql": "<a single read-only SELECT query>", "explanation": "<one sentence on what it computes>"}. \
Never use INSERT/UPDATE/DELETE/DROP/ALTER/CREATE/ATTACH/COPY/PRAGMA. Always fully qualify table \
names as schema.table (e.g. gold.orders_by_category). If the question can't be answered from the \
schema, return {"sql": "", "explanation": "<why not>"}."""


class QueryAgentError(RuntimeError):
    """Raised when a question can't be safely turned into a query, or
    when no Anthropic API key is configured."""


class QueryAgent:
    """Turns natural-language questions into read-only SQL over the vault."""

    def __init__(self, vault: MedallionVault, model: str = "claude-sonnet-4-6",
                 api_key: str | None = None) -> None:
        self.vault = vault
        self.model = model
        self.api_key = api_key or os.getenv("ANTHROPIC_API_KEY")

    def _schema_context(self) -> str:
        lines = []
        for schema in ("bronze", "silver", "gold"):
            for table in self.vault.list_tables(schema):
                try:
                    sample = self.vault.query(f"SELECT * FROM {schema}.{table} LIMIT 0")
                    cols = ", ".join(f"{c} ({t})" for c, t in zip(sample.columns, sample.dtypes))
                except Exception:  # noqa: BLE001 - skip tables that fail introspection
                    cols = "unknown"
                lines.append(f"{schema}.{table}: {cols}")
        return "\n".join(lines) if lines else "(no tables populated yet)"

    def _generate_sql(self, question: str) -> dict[str, str]:
        if anthropic is None:
            raise QueryAgentError(
                "QueryAgent requires the `anthropic` package. "
                "Install with: pip install -r requirements-extra.txt"
            )
        if not self.api_key:
            raise QueryAgentError(
                "QueryAgent requires ANTHROPIC_API_KEY to be set in the environment."
            )

        client = anthropic.Anthropic(api_key=self.api_key)
        prompt = f"Schema:\n{self._schema_context()}\n\nQuestion: {question}"
        response = client.messages.create(
            model=self.model, max_tokens=500, system=_SYSTEM_PROMPT,
            messages=[{"role": "user", "content": prompt}],
        )
        text = "".join(block.text for block in response.content if getattr(block, "text", None))
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError as exc:
            raise QueryAgentError(f"Model did not return valid JSON: {text!r}") from exc
        return parsed

    @staticmethod
    def _validate_sql(sql: str) -> None:
        stripped = sql.strip().rstrip(";")
        if not stripped:
            raise QueryAgentError("Empty query.")
        if not stripped.lower().startswith("select"):
            raise QueryAgentError("Only SELECT queries are permitted.")
        if _FORBIDDEN.search(stripped):
            raise QueryAgentError("Query contains a disallowed statement.")

    def ask(self, question: str) -> dict[str, Any]:
        """Answers a natural-language question against the warehouse.

        Returns a dict with ``sql``, ``explanation``, and ``result``
        (a Polars DataFrame) — or raises ``QueryAgentError`` if the
        question can't be safely answered.
        """
        generated = self._generate_sql(question)
        sql = generated.get("sql", "")
        explanation = generated.get("explanation", "")
        if not sql:
            raise QueryAgentError(explanation or "The model could not answer this question.")

        self._validate_sql(sql)
        result = self.vault.query(sql)
        return {"question": question, "sql": sql, "explanation": explanation, "result": result}
