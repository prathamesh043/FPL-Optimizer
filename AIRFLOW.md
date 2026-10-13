# Airflow 3 (Docker)

Local Apache Airflow **3.0.2** setup for the FPL Optimizer ETL DAG (`dags/fpl_etl_dag.py`).

## Prerequisites

- Docker Desktop (or Docker Engine + Compose v2)
- At least **4 GB** RAM allocated to Docker (8 GB recommended)

## Setup

1. Copy env template and fill in Supabase credentials:

```bash
cp .env.example .env
```

2. Build the custom Airflow image (includes pandas, psycopg2, standard/FAB providers, etc.):

```bash
docker compose build
```

3. Initialize the Airflow database and admin user (once):

```bash
docker compose up airflow-init
```

4. Start Airflow:

```bash
docker compose up
```

Or run detached: `docker compose up -d`

## Upgrading from Airflow 2.x

Airflow 3 uses a different metadata schema and component layout. Wipe the old stack first:

```bash
docker compose down --volumes
docker compose build --no-cache
docker compose up airflow-init
docker compose up
```

## Access

| | |
|---|---|
| UI | http://localhost:8080 |
| User | `airflow` |
| Password | `airflow` |

The DAG `fpl_etl_dag` is paused on creation. Unpause it in the UI to run on the schedule (`0 */3 * * *`), or trigger it manually.

## Layout

| Path | Role |
|---|---|
| `docker-compose.yaml` | Postgres + api-server + scheduler + dag-processor (LocalExecutor) |
| `Dockerfile` | Extends `apache/airflow:3.0.2` with project deps |
| `requirements-airflow.txt` | Extra Python packages for DAGs |
| `dags/` | DAG definitions (mounted as `/opt/airflow/dags`) |
| `squad_optimization/` | Shared modules (`config`, optimizer helpers) |
| `logs/` | Task and scheduler logs |
| `plugins/` | Custom Airflow plugins |
| `config/` | Airflow config (`airflow.cfg` seeded on init) |

## Useful commands

```bash
# Stop stack (keep DB volume)
docker compose down

# Stop and wipe Airflow metadata DB
docker compose down --volumes

# Rebuild after changing requirements-airflow.txt
docker compose build --no-cache
docker compose up -d

# Airflow CLI via api-server container
docker compose exec airflow-apiserver airflow version
docker compose exec airflow-apiserver airflow dags list
docker compose exec airflow-apiserver airflow dags trigger fpl_etl_dag
```

## When do I need to restart?

| Change | Action needed |
|---|---|
| Edit a file in `dags/` | None. Airflow re-parses it automatically (mounted volume). |
| Edit `.env` (e.g. `SUPABASE_*`) | Recreate containers: `docker compose up -d --force-recreate` |
| Edit `docker-compose.yaml` | `docker compose up -d` (changed services are recreated) |
| Edit `requirements-airflow.txt` or `Dockerfile` | Rebuild, then restart: `docker compose build && docker compose up -d` |
| Edit `squad_optimization/` modules | Usually none for new task runs. Restart if a module is imported by the DAG file at parse time: `docker compose restart airflow-dag-processor` |
| Airflow is unresponsive or stuck | `docker compose restart` |
| Airflow upgrade or corrupted metadata DB | Full reset (see "Upgrading from Airflow 2.x") |

Note: `docker compose restart` does **not** reload `.env`. Env vars are fixed when a container is created, so use `up -d --force-recreate` instead.

## Notes

- Airflow 3 replaces the webserver with `airflow-apiserver` and adds a separate `airflow-dag-processor`.
- `AIRFLOW__API_AUTH__JWT_SECRET` must be the same across all services (set in `.env`).
- Credentials come from `.env` → Compose → `SUPABASE_*` env vars → `squad_optimization/config.py`.
- Do not commit a filled-in `.env`.
