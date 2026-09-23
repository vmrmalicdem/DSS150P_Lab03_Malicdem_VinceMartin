# DSS150P Laboratory 3 - Productionizing a Modular Data Pipeline

Implements a raw -> staging -> curated -> PostgreSQL pipeline for e-commerce sales
order lines, benchmarked across CSV/JSON/Parquet/PostgreSQL, partitioned by
order_year/order_month, and orchestrated with Apache Airflow.

## Structure
- Goal 1: reproducible environment, modularization, Git, Docker, configuration
- Goal 2: raw -> staging -> curated transformations, quarantine, rerun-safe UPSERT
- Goal 3: CSV/JSON/Parquet/PostgreSQL benchmark, partitioning, selected-partition load
- Goal 4: Apache Airflow DAG (extract -> transform -> load/load-partition -> validate)

See `docs/DESIGN_DECISIONS.md`, `docs/benchmark_interpretation.md`, and
`docs/technical_reflection.md` for write-ups; `docs/run_evidence.md` for run evidence.

## Setup

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip setuptools wheel
pip install -r requirements.txt
```

Create `.env` (no `.env.example` ships in this repo) with:POSTGRES_HOST=localhost
POSTGRES_PORT=5433
POSTGRES_DB=dss150p
POSTGRES_USER=dss150p
POSTGRES_PASSWORD=change_me
PIPELINE_RUN_ID= 
Note: `POSTGRES_PORT=5433`, not the usual 5432. This machine has a native Windows
PostgreSQL service already bound to 5432, so `docker-compose.yml` maps the container's
5432 to host port 5433 instead (`"5433:5432"`) to avoid the conflict. Inside the Docker
network, containers still reach Postgres via `postgres:5432` (the service name and its
real internal port) -- `docker-compose.airflow.yml` overrides `POSTGRES_HOST` and
`POSTGRES_PORT` for the Airflow containers accordingly, so this does not need to be
set manually there.

```powershell
python -m src.cli validate-env
```

## Pipeline commands

```powershell
python -m src.cli extract                          # copy data/source/ into a run-specific raw snapshot
python -m src.cli transform                         # raw -> staging -> curated, quarantine written
python -m src.cli validate                          # data-quality checks on curated output
python -m src.cli load                               # rerun-safe UPSERT into curated.sales_order_lines
python -m src.cli benchmark --repeats 5              # CSV/JSON/Parquet/Postgres comparison + partitioned Parquet
python -m src.cli load-partition --year 2026 --month 1   # load one partition + audit.partition_loads
python -m src.cli run-all                             # extract+transform+load+validate, one run_id, audit.pipeline_runs row
```

## Docker / PostgreSQL

```powershell
docker compose build pipeline
docker compose up -d postgres
docker compose ps
docker exec -it dss150p-postgres psql -U dss150p -d dss150p -c "\dn"
```

## Airflow (Goal 4)

```powershell
docker compose -f docker-compose.yml -f docker-compose.airflow.yml build
docker compose -f docker-compose.yml -f docker-compose.airflow.yml up airflow-init
docker compose -f docker-compose.yml -f docker-compose.airflow.yml up -d airflow-webserver airflow-scheduler
```

UI: http://localhost:8080 (admin/admin, training credentials only).

DAG `dss150p_sales_pipeline`: daily schedule `0 2 * * *`, catchup disabled. Params:
`run_mode` (`full` or `partition`), `year`, `month`. Branches after `transform` into
`load_full` or `load_partition` based on `run_mode`; `validate` runs regardless of
which branch executed.

## Tests

```powershell
python -m pytest tests\ -v
```
