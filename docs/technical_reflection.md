# Technical Reflection

## Modularity
Splitting the pipeline into separate modules paid off when I hit the Airflow run_id colon
bug: the fix was one line in extract/files.py, nothing else needed to change. My DAG just
calls python -m src.cli <command> per stage. No business logic duplicated there.

## Idempotency
I verified this directly: I ran load and load-partition each twice against real Postgres,
and total equaled distinct_orders (49,897) both times. This works because I excluded
pipeline_run_id and processed_at_utc from record_hash, since both change every run
regardless of whether the order itself changed. Including them would make every rerun
look like a real update. The ON CONFLICT ... WHERE record_hash <> EXCLUDED.record_hash
clause is what makes a retried load safe to repeat.

## Storage trade-offs
I benchmarked on the full 49,897-row dataset. Parquet came out smallest (5.5MB vs CSV
15MB, JSON 30MB) and fastest to read. Postgres matched Parquet on filtered reads (0.10s)
even with no index on status, so I expect adding one would put it ahead. The real lesson
I took from this: storage choice depends on whether I can index for my access pattern,
not just file size.

## Orchestration vs. business logic
Every task in my DAG is a thin BashOperator call into src.cli. The only Airflow-specific
code I wrote is the branching logic and the failure callback. All order/customer/product
logic lives in src/, so I can run and test it outside Airflow entirely. That is what made
debugging possible: I could check whether python -m src.cli <command> worked standalone
before blaming Airflow.
