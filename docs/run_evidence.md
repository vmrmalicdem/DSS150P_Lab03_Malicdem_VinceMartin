# Run Evidence

## Week 4
- Python version: 3.13 (Windows), venv at .venv
- Git status/log evidence: commits include initial env setup, __pycache__ cleanup (retroactive .gitignore fix), Goal 2/3 implementation, and run-all wiring -- see `git log --oneline`
- Docker image/container evidence: dss150p-postgres container running postgres:16, schemas `audit`, `curated`, `staging` confirmed via `\dn`; tables `curated.sales_order_lines`, `audit.pipeline_runs`, `audit.partition_loads` confirmed via `\dt`
- External configuration evidence: non-secret defaults in config/settings.yml, secrets/environment-specific values in .env (gitignored, confirmed via `git check-ignore .env`), src/config.py is the only place that reads both

## Week 5
- Raw row counts: extract copies data/source/ files unmodified into data/raw/run_id=<run_id>/ -- row counts unchanged from source at this step (customers ~3,000 + duplicate versions, products ~600 + duplicates, orders ~50,000 + duplicates, per docs/DATASET_GUIDE.md)
- Staging row counts: 53,597 (customers + products + orders combined, after dedup-by-latest-updated_at and quarantine removal)
- Curated row counts: 49,897
- Quarantine row counts: 104 total -- 100 orphan_product_reference, 1 orphan_customer_reference, 1 invalid_or_negative_price, 1 invalid_quantity, 1 invalid_status
- First load affected rows: 49,897 upserted into curated.sales_order_lines
- Second rerun affected rows / evidence of idempotency: 49,897 upserted again; verified via `SELECT COUNT(*) total, COUNT(DISTINCT order_id) distinct_orders FROM curated.sales_order_lines` returning total=distinct_orders=49,897 both times -- no duplicate order_id values created on rerun

## Week 6
- Benchmark table attached: yes, data/benchmarks/benchmark_results.csv
  - csv: 15,276,675 bytes, write 1.35s, full read 0.30s, filtered read 0.27s
  - json_lines: 30,707,468 bytes, write 0.93s, full read 0.75s, filtered read 0.91s
  - parquet_snappy: 5,466,903 bytes, write 0.26s, full read 0.08s, filtered read 0.06s
  - postgresql: 16,564,224 bytes (pg_total_relation_size), full read 0.84s, filtered read 0.10s (no index on status)
- Partition selected: 2026-01
- Partition row count: 2,506
- PostgreSQL verification query: `SELECT * FROM audit.partition_loads WHERE partition_key = '2026-01';` -> 1 row, row_count=2506

## Week 7
- DAG ID: dss150p_sales_pipeline
- Schedule: 0 2 * * * (daily 02:00 UTC), catchup=False
- Parameters used: run_mode (full/partition, tested both), year=2026, month=1 for partition mode
- Successful run ID: manual__2026-09-23T07:36:10+00:00 (run_mode=partition, all tasks green, load_partition executed, load_full skipped)
- Deliberate failure run ID: manual__2026-09-23T07:37:51+00:00 (data/source/orders.csv renamed away before trigger)
- Retry/failure-handling evidence: extract task failed 3 total attempts (1 initial + 2 retries per DEFAULT_ARGS), final failure log shows FileNotFoundError for data/source/orders.csv; custom failure_callback printed dag_run/task/try/exception context each attempt; task instance marked FAILED after try=3
- Final recovery run ID: manual__2026-09-23T07:37:51+00:00 (same run) -- orders.csv restored, extract task cleared via Airflow UI, full DAG reran and completed with all tasks green (load_partition skipped, this was a run_mode=full retry)
