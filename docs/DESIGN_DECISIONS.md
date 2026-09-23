
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
