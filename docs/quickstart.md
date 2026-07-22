# Quickstart

## 1. Environment

```bash
git clone https://github.com/yourusername/agentic-aegis.git
cd agentic-aegis
python -m venv venv
source venv/bin/activate          # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

## 2. Initialize

```bash
python scripts/setup_initial.py
```

Creates `data/raw`, `data/bronze`, `data/silver`, `data/gold`, `logs/`, and
an empty `data/foundry.duckdb` with the bronze/silver/gold schemas.

## 3. Generate sample data

```bash
python scripts/generate_sample_data.py --rows 1000 --seed 42
```

Produces `data/raw/orders.csv` — a synthetic e-commerce dataset with
deliberately injected nulls, whitespace, negative amounts, case
inconsistencies, malformed emails, outliers, and duplicate rows, so the
agents have real problems to find.

## 4. Run the pipeline

```bash
python -m src.orchestration.foundry_flows
```

This runs Scout → Sentinel → Healer → Oracle, then writes Bronze, Silver,
and Gold tables into `data/foundry.duckdb`, and records the run in
`pipeline_runs` + `data_lineage`.

## 5. Launch the dashboard

```bash
streamlit run dashboards/foundry_dashboard.py
```

Visit http://localhost:8080.

## Troubleshooting

| Symptom | Fix |
|---|---|
| `FileNotFoundError: No raw data found at data/raw/orders.csv` | Run step 3 first. |
| Dashboard shows "No Bronze/Silver/Gold tables yet" | Run step 4 first — the dashboard only reads from `data/foundry.duckdb`, it never generates data. |
| `ModuleNotFoundError` for `src` | Run commands from the repo root, not from inside `scripts/` or `dashboards/`. |
| Prefect logs are noisy | Set `PREFECT_LOGGING_LEVEL=WARNING` before running the flow. |
| Want a bigger dataset | `python scripts/generate_sample_data.py --rows 50000` |
