# Deployment

## Local (development)

Covered in [`quickstart.md`](quickstart.md) — a `venv` plus
`streamlit run` is sufficient for local use; DuckDB needs no server.

## Docker

A [`Dockerfile`](../Dockerfile) at the repo root builds the dashboard
image (installs both `requirements.txt` and `requirements-extra.txt`,
so connectors/observability/NL-query/streaming all work out of the box):

```bash
docker build -t agentic-aegis .
docker run -p 8080:8080 -v $(pwd)/data:/app/data agentic-aegis
```

Mounting `./data` keeps the DuckDB file (and generated CSVs) persistent
across container restarts. Run the pipeline instead of the dashboard
with:

```bash
docker run --rm -v $(pwd)/data:/app/data agentic-aegis \
  python -m src.orchestration.foundry_flows
```

### docker compose (local dev with a real Postgres)

[`docker-compose.yml`](../docker-compose.yml) brings up the dashboard
plus a throwaway Postgres instance, so you can exercise `PostgresSource`
without provisioning real infrastructure:

```bash
docker compose up --build
docker compose run --rm aegis python scripts/generate_sample_data.py --rows 1000
docker compose run --rm aegis python -m src.orchestration.foundry_flows
```

## Kubernetes

[`k8s/`](../k8s) has a complete, minimal manifest set:

| File               | Purpose                                                  |
|--------------------|-----------------------------------------------------------|
| `namespace.yaml`   | `agentic-aegis` namespace                                  |
| `configmap.yaml`   | non-secret settings + a Secret **template** (apply your own real Secret, don't commit values) |
| `pvc.yaml`         | persistent volume for the DuckDB file                      |
| `rbac.yaml`        | least-privilege ServiceAccount/Role/RoleBinding — read-only access to its own ConfigMap/Secret, nothing cluster-wide |
| `deployment.yaml`  | the Streamlit dashboard, with readiness/liveness probes     |
| `cronjob.yaml`     | scheduled pipeline runs (2am daily by default), sharing the dashboard's PVC |

```bash
kubectl apply -f k8s/namespace.yaml
kubectl apply -f k8s/pvc.yaml
kubectl apply -f k8s/rbac.yaml
kubectl create secret generic aegis-secrets -n agentic-aegis \
  --from-literal=AEGIS_PG_PASSWORD=*** \
  --from-literal=ANTHROPIC_API_KEY=*** \
  --from-literal=AEGIS_ALERT_WEBHOOK_URL=***
kubectl apply -f k8s/configmap.yaml   # ConfigMap only; skip the Secret block if you applied your own above
kubectl apply -f k8s/deployment.yaml
kubectl apply -f k8s/cronjob.yaml
```

DuckDB is single-writer, so `deployment.yaml` runs one replica by
design — scale the dashboard only after moving to a shared backend
(see "Cloud notes" below).

## CI/CD

[`.github/workflows/ci.yml`](../.github/workflows/ci.yml) runs on every
push/PR to `main`:

1. **Lint** — `ruff check .`
2. **Test** — the full `pytest` suite across Python 3.10/3.11/3.12, with
   both `requirements.txt` and `requirements-extra.txt` installed
3. **Docker build smoke test** — confirms the image still builds

## Scheduling pipeline runs (Prefect, non-K8s)

For a periodic pipeline outside Kubernetes, register the flow with a
Prefect deployment:

```bash
prefect deployment build src/orchestration/foundry_flows.py:run_foundry_pipeline \
  -n "aegis-nightly" --cron "0 2 * * *"
prefect deployment apply run_foundry_pipeline-deployment.yaml
prefect agent start -q default
```

This runs the full Scout → Sentinel → Healer → Oracle → Bronze/Silver/Gold
pipeline every night at 2am and keeps `pipeline_runs` / `data_lineage`
populated for the dashboard.

## Cloud notes

- **Compute**: any container platform (ECS, Cloud Run, an Azure Container
  App, a small VM, or Kubernetes via the manifests above) works — there's
  no stateful server process besides the DuckDB file itself.
- **Storage**: for multi-instance deployments, point `DB_PATH` at a shared
  volume, or swap `MedallionVault`'s DuckDB connection for DuckDB's
  MotherDuck-hosted mode if you need a managed, shared backend.
- **Secrets**: none required for the default sample-data flow. Once you
  wire in real data sources (S3, Postgres, MySQL) or the NL-query agent,
  keep credentials in environment variables (or a Kubernetes Secret) and
  never in source control — see [`connectors.md`](connectors.md) and
  [`observability.md`](observability.md) for the exact variables each
  feature reads.
