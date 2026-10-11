# Goal 3 Benchmark Interpretation

Machine: Intel(R) Core(TM) i7-8665U CPU @ 1.90GHz, 16 GB RAM, Microsoft Windows 11 Pro, Python 3.13.0

Dataset: 49,897 curated rows. 8,355 rows (16.7%) have status DELIVERED. Every timing,
including writes, is the median of 5 runs. These numbers are from this machine only.
PostgreSQL 16 runs in a Docker container on the same laptop, so its numbers include
Docker Desktop and driver overhead. Raw results: data/benchmarks/benchmark_results.csv.

## Results
| Format           | Size (bytes) | Write (s) | Full read (s) | Filtered read (s) |
|------------------|--------------|-----------|---------------|-------------------|
| CSV              | 15,276,675   | 1.27      | 0.44          | 0.38              |
| JSON Lines       | 30,707,468   | 1.00      | 1.05          | 0.81              |
| Parquet (snappy) | 5,466,903    | 0.18      | 0.07          | 0.03              |
| PostgreSQL       | 16,580,608   | 6.25      | 0.59          | 0.10              |

Notes on the PostgreSQL row:
- Size is pg_total_relation_size (table plus indexes), a different measurement from file size.
- Write is the pipeline's own UPSERT of all rows into an empty scratch table. It goes row by
  row through Python, so it measures the pipeline's write path, not Postgres storage speed.
  A bulk COPY would be faster but cannot apply the record_hash skip logic.
- Reads return Python tuples while the file formats return DataFrames, so the read columns
  are not strictly like for like.

## 1. Smallest format
Parquet at 5.5 MB, versus CSV at 15.3 MB (2.8x larger) and JSON Lines at 30.7 MB (5.6x
larger). Parquet stores each column together as typed binary and compresses it with snappy.
CSV stores everything as text, and JSON Lines also repeats every field name on every row.

## 2. Fastest full read
Parquet at 0.07 s, versus CSV at 0.44 s and JSON Lines at 1.05 s. That does not make it best
for every workload. Parquet is poor for appending single rows, cannot be read in a text
editor, and some tools cannot read it. CSV stays useful for exchange and quick inspection.

## 3. Filtered retrieval: Parquet vs PostgreSQL
Parquet took 0.03 s and PostgreSQL 0.10 s. Parquet reads typed binary columns with no text
parsing, while the PostgreSQL query has no index on status and scans the table. An index
could help, but 16.7% of rows match, and PostgreSQL often prefers a sequential scan when that
many rows qualify. I did not test an index, so I cannot claim it would close the gap.
EXPLAIN ANALYZE before and after creating one would show it.

## 4. JSON Lines vs one JSON array
One record per line lets a consumer process the file incrementally and append records without
rewriting it, and a corrupted line costs only that record. A single JSON array cannot be
parsed until the closing bracket, appending means rewriting the end of the file, and one bad
byte can invalidate the whole file.

## 5. High cardinality or poor locality partition keys
A key with very high cardinality, such as customer_id (about 3,000 values), would create
thousands of tiny files and directories, and the metadata and file-open overhead would
outweigh the I/O saved. A key the queries do not filter on, such as year/month when queries
filter by customer, still scans every partition, so the overhead buys nothing.

## Partitioning evidence
The data is partitioned by order_year/order_month into 21 partitions (2025-01 to 2026-09),
about 2,400 rows each. Reading partition 2026-01 touches 2,506 of 49,897 rows (5%). The
loader checks that every row in the partition belongs to the requested month and that
order_id is unique before loading anything. Loaded into an empty table: 2,506 rows written,
a rerun wrote 0, and audit.partition_loads holds one row with row_count 2,506. At this data
size partitioning mostly adds overhead (21 small files). The benefit appears when data is
large and queries filter on year and month. The tree is rebuilt on every use, because
pyarrow names files uniquely and writing into an old tree would duplicate rows.
