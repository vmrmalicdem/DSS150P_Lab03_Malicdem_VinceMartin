**1.** Parquet is smallest (5.47MB vs CSV 15.28MB, JSON 30.71MB) because it's columnar with snappy compression, while JSON repeats every field name on every row.

**2.** Parquet was fastest to read fully (0.082s vs CSV 0.299s, JSON 0.750s), but that doesn't hold for every workload — something needing human-readable output or no Parquet support would still reach for CSV.

**3.** Parquet filtered read (0.055s) beat Postgres (0.103s) because Postgres had no index on `status` and did a sequential scan; adding `CREATE INDEX ON curated.sales_order_lines (status)` would likely close that gap.

**4.** JSON Lines lets a consumer process records one at a time as they arrive and isolates a corrupted line to just that record, while a single JSON array can't be parsed until the closing bracket and breaks entirely on one bad byte.

**5.** Too many tiny partitions add filesystem/metadata overhead that outweighs the I/O savings, and a query that filters on something other than the partition key still has to scan most partitions anyway.