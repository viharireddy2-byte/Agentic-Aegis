"""
One-time setup for Agentic Aegis: creates the data directory layout
and initializes an empty MedallionVault (bronze/silver/gold schemas +
observability tables) so the first pipeline run has somewhere to write.

Usage:
    python scripts/setup_initial.py
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.database import MedallionVault  # noqa: E402

REQUIRED_DIRS = ["data/raw", "data/bronze", "data/silver", "data/gold", "data/incoming", "logs"]


def main() -> None:
    print("Setting up Agentic Aegis ...")

    for d in REQUIRED_DIRS:
        Path(d).mkdir(parents=True, exist_ok=True)
        print(f"  ✓ ensured directory: {d}")

    vault = MedallionVault("data/foundry.duckdb")
    print("  ✓ initialized DuckDB medallion vault (bronze / silver / gold schemas)")
    vault.close()

    print("\nSetup complete. Next steps:")
    print("  1. python scripts/generate_sample_data.py")
    print("  2. python -m src.orchestration.foundry_flows")
    print("  3. streamlit run dashboards/foundry_dashboard.py")


if __name__ == "__main__":
    main()
