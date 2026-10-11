# DSS150P Laboratory 3: Productionizing a Modular Data Pipeline

A raw -> staging -> curated -> PostgreSQL pipeline for e-commerce sales order lines.
It is rerun-safe, benchmarked across CSV / JSON Lines / Parquet / PostgreSQL, partitioned
by order_year/order_month, and orchestrated with Apache Airflow.

## Requirements
Python 3.11 to 3.13, Git, Docker Desktop with Compose v2, and about 6 GB of free disk.

## Setup

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip setuptools wheel
pip install -r requirements.txt
copy .env.example .env
python -m src.cli validate-env
```

Edit `.env` and replace the training password. `.env` is gitignored.

`POSTGRES_PORT=5433` is deliberate. The container's Postgres is published on host port
5433 because this machine already runs a native Postgres on 5432. If your 5432 is free you
may use 5432 in both `docker-compose.yml` (`"5432:5432"`) and `.env`. Containers always
reach Postgres at `postgres:5432` inside the Docker network, and the compose files set
`POSTGRES_HOST` and `POSTGRES_PORT` for them, so `.env` only affects commands run on the host.

## Docker and PostgreSQL

```powershell
docker compose build pipeline
docker compose up -d postgres
docker compose ps
docker compose run --rm pipeline python -m src.cli validate-env
docker exec -it dss150p-postgres psql -U dss150p -d dss150p -c "\dn"
```

The schemas `audit`, `curated` and `staging` are created by `sql/init/` on the first start
of an empty volume. If you change the database password after the first start, remove the
volume (`docker compose down -v`) so Postgres reinitializes with the new one.

## Pipeline commands

```powershell
python -m src.cli extract                                # copy data/source into a run-specific raw snapshot
python -m src.cli transform                              # raw -> staging -> curated; rejected rows go to quarantine
python -m src.cli validate                               # data-quality checks on the curated output
python -m src.cli load                                   # rerun-safe UPSERT into curated.sales_order_lines
python -m src.cli benchmark --repeats 5                  # compare formats, write data/benchmarks/benchmark_results.csv
python -m src.cli load-partition --year 2026 --month 1   # load one verified partition, record it in audit.partition_loads
python -m src.cli run-all                                # extract, transform, load, validate; logs to audit.pipeline_runs
```

`load` and `load-partition` print the number of rows actually written, not the number
submitted. A rerun on unchanged data prints 0 because `record_hash` marks every row as
unchanged.

## Airflow

```powershell
docker compose -f docker-compose.yml -f docker-compose.airflow.yml build
docker compose -f docker-compose.yml -f docker-compose.airflow.yml up airflow-init
docker compose -f docker-compose.yml -f docker-compose.airflow.yml up -d airflow-webserver airflow-scheduler
```

UI: http://localhost:8081 (login admin / admin, training credentials only). It is published
on 8081 because 8080 was already in use on this machine. To use 8080, change `"8081:8080"`
in `docker-compose.airflow.yml`.

DAG `dss150p_sales_pipeline` runs daily at 02:00 UTC with `catchup=False` and
`max_active_runs=1`, so runs never overlap. Params: `run_mode` (`full` or `partition`),
`year` and `month`. After `transform` it branches to `load_full` or `load_partition`, and
`validate` runs after whichever branch executed. Retries: 2, with a 1 minute delay.

## Tests

```powershell
python -m pytest tests -v
```

## Reproducing the evidence
Empty the tables, then run `run-all` (writes 49,897 rows), then `load` twice (writes 0 each
time). Step by step results are in `docs/run_evidence.md`.

## Layout
- `src/`: extract, transform, load, validate, benchmark, `cli.py` (thin entry points), `config.py`
- `dags/`: the Airflow DAG, which only calls the CLI
- `config/settings.yml`: non-secret settings; `.env`: secrets and per-machine values
- `data/source/` is committed. The other `data/` folders are generated and gitignored,
  except `data/benchmarks/benchmark_results.csv`, which is a required deliverable.
- `docs/`: design decisions, benchmark interpretation, technical reflection, run evidence, screenshots

## Known limitations
- Output files use fixed paths. `max_active_runs=1` prevents two DAG runs from overlapping,
  but running a CLI command by hand while a DAG run is active can still collide.
- The PostgreSQL write time in the benchmark measures the pipeline's row by row UPSERT,
  not a bulk load, so it is not a pure storage comparison.

## AI assistance
I used Claude (Anthropic) for code generation, debugging and drafting documentation in this
lab. I ran and verified the pipeline, tests, Docker setup and Airflow runs myself and I
am responsible for everything submitted.
