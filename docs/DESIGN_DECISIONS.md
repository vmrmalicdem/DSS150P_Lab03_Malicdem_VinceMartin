
## Duplicate detection compares against current DB state
upsert_curated compares the incoming record_hash against what is currently stored in
curated.sales_order_lines, not a separately tracked "last seen hash." This avoids a
real failure mode: if that tracking drifts, a legitimate update could get wrongly
treated as a duplicate and rejected.

## Writes are atomic (temp file + os.replace)
Staging/curated Parquet, quarantine CSV, and the raw-run pointer file all write to a
.tmp sibling first, then os.replace() moves it into place. Atomic on Windows and POSIX,
so a reader never sees a partial file even if the process is killed mid-write. See
src/common/atomic_io.py.

## upsert_curated reports rows actually written, not rows submitted
Uses RETURNING order_id and counts the result set instead of len(records). Postgres
skips RETURNING for rows where ON CONFLICT DO UPDATE ... WHERE is false, so the count
naturally excludes no-op rows. Verified: rerun on unchanged data reported 0 written;
after manually corrupting record_hash for 3 rows in Postgres, a rerun reported 3.

## Overlapping DAG runs
The DAG sets max_active_runs=1, so a second run (manual or scheduled) queues until the first
finishes. Staging, curated and quarantine outputs use fixed paths, so overlapping runs would
overwrite each other's files. Limitation: this only protects DAG runs from each other. Running
a CLI command by hand during a DAG run can still collide. Run-specific output paths would
also require load, validate and benchmark to be told which run to read.

## Partition tree is rebuilt on every use
pyarrow gives each partition file a new unique name, so writing into an existing tree adds
files and duplicates every row. write_partitioned_parquet builds the tree in a temp directory
and swaps it in. read_partition checks that every row belongs to the requested month and that
order_id is unique before anything is loaded.

## Ports
Postgres is published on host port 5433 (a native Postgres already uses 5432) and Airflow on
8081 (another project's Airflow uses 8080). Containers use postgres:5432 inside the Docker
network, set explicitly in both compose files so .env cannot leak the host port into them.

## PostgreSQL write benchmark
Measures the pipeline's UPSERT of the full dataset into an empty scratch table with the same
structure and indexes, median of 5. It is the real write path, not a bulk-load comparison.
