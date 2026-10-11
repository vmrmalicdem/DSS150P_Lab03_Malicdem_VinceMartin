# Run Evidence

Machine: Intel(R) Core(TM) i7-8665U CPU @ 1.90GHz, 16 GB RAM, Microsoft Windows 11 Pro, Python 3.13.0

## Week 4: environment
- Python: Python 3.13.0 in .venv. requirements.txt pins pandas 2.2.3, pyarrow 17.0.0 (below Python 3.13) or 18 and up (3.13), psycopg 3.2.3, python-dotenv, PyYAML and pytest.
- Git: remote on GitHub, full history in git log --oneline. The branch goal1-reproducible-environment was created retroactively. My first commits landed on master.
- Docker: postgres:16 healthy on host port 5433 (container port 5432). Schemas audit, curated and staging exist. Tables: curated.sales_order_lines, audit.pipeline_runs, audit.partition_loads. The pipeline image builds and docker compose run --rm pipeline python -m src.cli validate-env succeeds.
- Configuration: non-secret settings in config/settings.yml, secrets and per-machine values in .env (gitignored, .env.example committed). src/config.py is the only module that reads both. The compose files set POSTGRES_HOST and POSTGRES_PORT for containers.

## Week 5: layers and rerun safety
- Raw row counts: customers=3003, products=601, orders=50005, total=53609. Extract copies the source files byte for byte, so counts are unchanged.
- Staging row counts: 53,597 (customers, products and orders after dedupe by latest updated_at and quarantine).
- Curated row counts: 49,897.
- Quarantine row counts: 104 (100 orphan_product_reference, 1 invalid_or_negative_price, 1 invalid_quantity, 1 invalid_status, 1 orphan_customer_reference).
- First load into an empty table (after TRUNCATE): run-all wrote 49,897 rows. The count is Postgres RETURNING, so it is rows actually written.
- Rerun on unchanged data: load run twice, each reported 0 rows written out of 49,897 submitted. COUNT(*) and COUNT(DISTINCT order_id) were both 49,897.
- Change detection: I set record_hash to a bogus value for 3 rows in Postgres and reran load, which wrote exactly 3. An earlier attempt that changed only status wrote 0. record_hash is stored and compared, not recomputed from the table, so the UPSERT detects changes in incoming source data, not manual edits to other columns.
- Error handling: data problems go to quarantine with a reason. System failures raise exceptions. run-all records FAILED or SUCCESS with row counts in audit.pipeline_runs.

## Week 6: benchmark and partition
- Benchmark table attached: yes, data/benchmarks/benchmark_results.csv. Medians of 5.
  - csv: 15,276,675 bytes, write 1.27 s, full read 0.44 s, filtered read 0.38 s
  - json_lines: 30,707,468 bytes, write 1.00 s, full read 1.05 s, filtered read 0.81 s
  - parquet_snappy: 5,466,903 bytes, write 0.18 s, full read 0.07 s, filtered read 0.03 s
  - postgresql: 16,580,608 bytes (pg_total_relation_size), write 6.25 s (pipeline UPSERT into an empty scratch table), full read 0.59 s, filtered read 0.10 s
- Partition selected: 2026-01, loaded into an emptied table. 2,506 rows read and verified, 2,506 written. The month query showed only 2026-01 with 2,506 rows. A rerun wrote 0 rows with the same counts.
- audit.partition_loads: one row, partition_key 2026-01, row_count 2506, pipeline_run_id run_20261010T154026Z_83adaaee.
- Verification query: SELECT to_char(order_timestamp, 'YYYY-MM') AS month, COUNT(*) FROM curated.sales_order_lines GROUP BY 1;

## Week 7: Airflow
- DAG ID: dss150p_sales_pipeline
- Schedule: 0 2 * * * (daily 02:00 UTC). Source files refresh daily, and an off-hours run leaves fresh data for the morning.
- catchup=False, because every run reads the same current source files, so backfilled runs would only reprocess identical data. max_active_runs=1, so runs queue instead of overlapping.
- Retries: 2, with a 1 minute delay. Execution timeout: 10 minutes per task.
- Parameters: run_mode (full or partition), year, month.
- Full run on the final DAG (run_mode=full): manual__2026-10-10T15:52:52+00:00, success.
- Overlap test: the same run was queued behind an active run and started when it finished (screenshot 06).
- Partition run (run_mode=partition, 2026-01): manual__2026-09-23T07:36:10+00:00. load_partition ran, load_full was skipped, and the audit row carries this run ID.
- Deliberate failure run: manual__2026-09-23T07:37:51+00:00. data/source/orders.csv was renamed away. extract failed with FileNotFoundError on tries 1 to 3, and the failure callback printed dag_run, task, try and exception on each failure.
- Redone drill: manual__2026-09-23T08:35:59+00:00, the same pattern.
- Recovery: orders.csv restored, extract cleared in the UI. In both runs extract succeeded on try 4 and the downstream tasks completed. No database cleanup was needed.
- Safe to rerun: extract (same run_id recopies the same files), transform (deterministic rebuild, atomic file replace), load (UPSERT with record_hash, 0 rows on unchanged data) and validate (read only).
- Earlier failed runs (scheduled__2026-09-22, manual__2026-09-23T03:01:14, manual__2026-09-23T03:05:09) were real bugs, a missing helper function and the host port leaking into the containers. Both were fixed. They are not part of the drill.
