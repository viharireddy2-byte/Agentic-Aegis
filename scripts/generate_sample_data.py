"""
Generates a synthetic e-commerce "orders" dataset with intentionally injected
quality issues, so the ScoutAgent / SentinelAgent / HealerAgent / OracleAgent
have something realistic to discover, score, fix, and flag.

Usage:
    python scripts/generate_sample_data.py [--rows 1000] [--seed 42]
"""

from __future__ import annotations

import argparse
import random
import string
from datetime import datetime, timedelta
from pathlib import Path

import polars as pl

CATEGORIES = ["Electronics", "Home & Kitchen", "Books", "Apparel", "Sports", "Toys", "Beauty"]
STATUSES = ["completed", "pending", "shipped", "cancelled", "returned"]


def _random_email(name: str) -> str:
    domain = random.choice(["example.com", "mail.com", "shop.io"])
    return f"{name.lower().replace(' ', '.')}@{domain}"


def _random_name() -> str:
    first = random.choice(["Alex", "Jordan", "Sam", "Taylor", "Morgan", "Casey", "Riley", "Jamie"])
    last = random.choice(["Kim", "Patel", "Garcia", "Chen", "Smith", "Nguyen", "Brown", "Silva"])
    return f"{first} {last}"


def generate_orders(n_rows: int, seed: int = 42) -> pl.DataFrame:
    random.seed(seed)
    start_date = datetime(2025, 1, 1)

    rows = []
    for i in range(n_rows):
        customer = _random_name()
        amount = round(random.uniform(5, 500), 2)
        order_date = start_date + timedelta(days=random.randint(0, 200))

        row = {
            "order_id": f"ORD-{10000 + i}",
            "customer_name": customer,
            "email": _random_email(customer),
            "category": random.choice(CATEGORIES),
            "amount": amount,
            "quantity": random.randint(1, 5),
            "status": random.choice(STATUSES),
            "order_date": order_date.strftime("%Y-%m-%d"),
        }
        rows.append(row)

    df = pl.DataFrame(rows)
    df = _inject_quality_issues(df, seed)
    return df


def _inject_quality_issues(df: pl.DataFrame, seed: int) -> pl.DataFrame:
    """Deliberately corrupts a slice of rows so agents have real work to do."""
    random.seed(seed + 1)
    n = df.height
    records = df.to_dicts()

    # 1. Nulls in email and category (~4%)
    for idx in random.sample(range(n), max(1, n // 25)):
        records[idx]["email"] = None
    for idx in random.sample(range(n), max(1, n // 40)):
        records[idx]["category"] = None

    # 2. Whitespace padding in customer_name (~3%)
    for idx in random.sample(range(n), max(1, n // 30)):
        records[idx]["customer_name"] = f"  {records[idx]['customer_name']}  "

    # 3. Negative amounts (refund artifacts) (~2%)
    for idx in random.sample(range(n), max(1, n // 50)):
        records[idx]["amount"] = -abs(records[idx]["amount"])

    # 4. Case inconsistency in category (~3%)
    for idx in random.sample(range(n), max(1, n // 30)):
        if records[idx]["category"]:
            records[idx]["category"] = records[idx]["category"].upper()

    # 5. Malformed emails (~2%)
    for idx in random.sample(range(n), max(1, n // 50)):
        if records[idx]["email"]:
            records[idx]["email"] = records[idx]["email"].replace("@", "_at_")

    # 6. Outlier amounts (~1%)
    for idx in random.sample(range(n), max(1, n // 100)):
        records[idx]["amount"] = round(records[idx]["amount"] * random.uniform(15, 25), 2)

    # 7. Duplicate rows (~2%)
    dupe_sources = random.sample(range(n), max(1, n // 50))
    for src in dupe_sources:
        records.append(dict(records[src]))

    return pl.DataFrame(records)


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate sample orders data for Agentic Aegis")
    parser.add_argument("--rows", type=int, default=1000, help="Number of base rows to generate")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for reproducibility")
    parser.add_argument("--out", type=str, default="data/raw/orders.csv", help="Output CSV path")
    args = parser.parse_args()

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    df = generate_orders(args.rows, args.seed)
    df.write_csv(out_path)

    print(f"Generated {df.height:,} rows (base {args.rows:,} + injected duplicates) -> {out_path}")
    print("Intentional issues injected: nulls, whitespace, negative amounts, "
          "case inconsistency, malformed emails, outliers, duplicates.")


if __name__ == "__main__":
    main()
