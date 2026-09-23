# Goal 3 Benchmark Interpretation

Ran on my machine (Windows, Python 3.13), full 49,897-row curated dataset, 5 repeats
per read. Results in benchmark_results.csv.

## Results
| Format          | Size (bytes) | Write (s) | Full read (s) | Filtered read (s) |
|------------------|--------------|-----------|-----------------|----------------------|
| CSV              | 15,276,675   | 1.35      | 0.30            | 0.27                 |
| JSON Lines       | 30,707,468   | 0.93      | 0.75            | 0.91                 |
| Parquet (snappy) | 5,466,903    | 0.26      | 0.08            | 0.06                 |
| PostgreSQL       | 16,564,224   | n/a       | 0.84            | 0.10                 |

## 1. Smallest format
Parquet (5.5MB vs CSV 15.3MB, JSON 30.7MB), because columnar storage with snappy
compression groups similar values per column. JSON is largest since it repeats every
field name on every row.

## 2. Fastest full read, best for every workload?
Parquet was fastest (0.08s). No, it does not generalize: no Parquet support, or a need
for human-readable output, still means CSV despite being slower.

## 3. Filtered retrieval: Parquet vs PostgreSQL
Parquet (0.06s) beat PostgreSQL (0.10s), but PostgreSQL had no index on status and did
a full scan. `CREATE INDEX ON curated.sales_order_lines (status)` would likely close or
flip that gap.

## 4. Why JSON Lines beats one JSON array for streaming
One record per line lets a consumer process incrementally and isolates a corrupted line
to one record. A single array cannot parse until the closing bracket, and one bad byte
breaks the whole file.

## 5. High-cardinality or poorly-localized partition keys
Too many tiny partitions add filesystem overhead that outweighs the I/O savings. A query
filtering on something other than the partition key still scans most partitions anyway.

## Partitioning evidence
Partitioned Parquet by order_year/order_month under data/partitioned/, spanning
2025-01 to 2026-09. Partition 2026-01 loaded via load-partition: 2,506 rows, recorded
in audit.partition_loads, verified rerun-safe (reran, row_count stayed 2,506, single
row with updated loaded_at_utc).
