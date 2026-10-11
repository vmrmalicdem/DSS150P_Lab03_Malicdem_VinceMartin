import shutil
import statistics
import time
from pathlib import Path

import pandas as pd

SCRATCH_TABLE = "curated.sales_order_lines_bench"


def _median_time(fn, repeats: int):
    times = []
    result = None
    for _ in range(repeats):
        start = time.perf_counter()
        result = fn()
        times.append(time.perf_counter() - start)
    return statistics.median(times), result


def _benchmark_csv(df, formats_dir, repeats, filter_status):
    path = formats_dir / "sales_order_lines.csv"
    write_seconds, _ = _median_time(lambda: df.to_csv(path, index=False), repeats)
    full_seconds, full_df = _median_time(lambda: pd.read_csv(path), repeats)
    filtered_seconds, filtered_df = _median_time(
        lambda: pd.read_csv(path).loc[lambda d: d["status"] == filter_status], repeats
    )
    return {
        "storage_type": "csv", "file_size_bytes": path.stat().st_size,
        "write_seconds": write_seconds, "full_read_seconds": full_seconds,
        "filtered_read_seconds": filtered_seconds, "row_count": len(full_df),
        "notes": f"all timings are medians of {repeats}; filtered rows={len(filtered_df)}",
    }


def _benchmark_jsonl(df, formats_dir, repeats, filter_status):
    path = formats_dir / "sales_order_lines.jsonl"
    write_seconds, _ = _median_time(
        lambda: df.to_json(path, orient="records", lines=True, date_format="iso"), repeats
    )
    full_seconds, full_df = _median_time(lambda: pd.read_json(path, lines=True), repeats)
    filtered_seconds, filtered_df = _median_time(
        lambda: pd.read_json(path, lines=True).loc[lambda d: d["status"] == filter_status], repeats
    )
    return {
        "storage_type": "json_lines", "file_size_bytes": path.stat().st_size,
        "write_seconds": write_seconds, "full_read_seconds": full_seconds,
        "filtered_read_seconds": filtered_seconds, "row_count": len(full_df),
        "notes": f"all timings are medians of {repeats}; filtered rows={len(filtered_df)}",
    }


def _benchmark_parquet(df, formats_dir, repeats, filter_status):
    path = formats_dir / "sales_order_lines.parquet"
    write_seconds, _ = _median_time(
        lambda: df.to_parquet(path, index=False, compression="snappy"), repeats
    )
    full_seconds, full_df = _median_time(lambda: pd.read_parquet(path), repeats)
    filtered_seconds, filtered_df = _median_time(
        lambda: pd.read_parquet(path, filters=[("status", "==", filter_status)]), repeats
    )
    return {
        "storage_type": "parquet_snappy", "file_size_bytes": path.stat().st_size,
        "write_seconds": write_seconds, "full_read_seconds": full_seconds,
        "filtered_read_seconds": filtered_seconds, "row_count": len(full_df),
        "notes": f"all timings are medians of {repeats}; filtered rows={len(filtered_df)}",
    }


def _time_postgres_writes(df, repeats):
    """Median time of the pipeline's real write path (UPSERT) into an empty
    scratch table with the same structure and indexes as the curated table."""
    from src.load.postgres import CURATED_TABLE, _connect, upsert_curated

    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {SCRATCH_TABLE}")
            cur.execute(f"CREATE TABLE {SCRATCH_TABLE} (LIKE {CURATED_TABLE} INCLUDING ALL)")
        conn.commit()

    times = []
    try:
        for _ in range(repeats):
            with _connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(f"TRUNCATE {SCRATCH_TABLE}")
                conn.commit()
            start = time.perf_counter()
            upsert_curated(df, "benchmark", table=SCRATCH_TABLE)
            times.append(time.perf_counter() - start)
    finally:
        with _connect() as conn:
            with conn.cursor() as cur:
                cur.execute(f"DROP TABLE IF EXISTS {SCRATCH_TABLE}")
            conn.commit()
    return statistics.median(times)


def _benchmark_postgres(df, repeats, filter_status):
    try:
        from src.load.postgres import CURATED_TABLE, _connect

        with _connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT pg_total_relation_size(%s)", (CURATED_TABLE,))
                size_bytes = cur.fetchone()[0]

                def full_query():
                    cur.execute(f"SELECT * FROM {CURATED_TABLE}")
                    return cur.fetchall()

                def filtered_query():
                    cur.execute(
                        f"SELECT * FROM {CURATED_TABLE} WHERE status = %s", (filter_status,)
                    )
                    return cur.fetchall()

                full_seconds, full_rows = _median_time(full_query, repeats)
                filtered_seconds, filtered_rows = _median_time(filtered_query, repeats)

        write_seconds = _time_postgres_writes(df, repeats)
        return {
            "storage_type": "postgresql", "file_size_bytes": size_bytes,
            "write_seconds": write_seconds, "full_read_seconds": full_seconds,
            "filtered_read_seconds": filtered_seconds, "row_count": len(full_rows),
            "notes": (
                f"all timings are medians of {repeats}; size = pg_total_relation_size (table + indexes); "
                f"write = pipeline UPSERT of the full dataset into an empty scratch table "
                f"(includes connection and row conversion, so not a pure storage comparison); "
                f"reads return python tuples, not a DataFrame; filtered rows={len(filtered_rows)}"
            ),
        }
    except Exception as exc:
        return {
            "storage_type": "postgresql", "file_size_bytes": None,
            "write_seconds": None, "full_read_seconds": None,
            "filtered_read_seconds": None, "row_count": None,
            "notes": f"postgresql benchmark failed: {exc}",
        }


def write_partitioned_parquet(df, output_dir):
    """Write Parquet partitioned by order_year/order_month, replacing any old tree.

    pyarrow gives every partition file a fresh uuid name, so writing into an
    existing tree would keep the old files and duplicate every row. Build the new
    tree in a temp directory, then swap it in.
    """
    df = df.copy()
    order_ts = pd.to_datetime(df["order_timestamp"], utc=True)
    df["order_year"] = order_ts.dt.year
    df["order_month"] = order_ts.dt.month

    output_dir = Path(output_dir)
    tmp_dir = output_dir.with_name(output_dir.name + ".tmp")
    if tmp_dir.exists():
        shutil.rmtree(tmp_dir)
    df.to_parquet(
        tmp_dir, index=False,
        partition_cols=["order_year", "order_month"], compression="snappy",
    )
    if output_dir.exists():
        shutil.rmtree(output_dir)
    tmp_dir.rename(output_dir)
    return output_dir


def read_partition(partition_dir, year: int, month: int) -> pd.DataFrame:
    """Read only the selected partition and verify its rows belong to it."""
    df = pd.read_parquet(
        partition_dir,
        filters=[("order_year", "==", year), ("order_month", "==", month)],
    )
    if df.empty:
        return df

    order_ts = pd.to_datetime(df["order_timestamp"], utc=True)
    outside = ~((order_ts.dt.year == year) & (order_ts.dt.month == month))
    if outside.any():
        raise RuntimeError(
            f"{int(outside.sum())} rows outside {year}-{month:02d} found in that partition"
        )
    if df["order_id"].duplicated().any():
        raise RuntimeError(f"duplicate order_id values found in partition {year}-{month:02d}")
    return df


def run_benchmark(curated_path, output_dir, repeats: int = 5) -> pd.DataFrame:
    from src.config import SETTINGS, path_for

    filter_status = SETTINGS["storage_benchmark"]["filter_status"]
    df = pd.read_parquet(curated_path)

    output_dir = Path(output_dir)
    formats_dir = output_dir / "formats"
    formats_dir.mkdir(parents=True, exist_ok=True)

    results = [
        _benchmark_csv(df, formats_dir, repeats, filter_status),
        _benchmark_jsonl(df, formats_dir, repeats, filter_status),
        _benchmark_parquet(df, formats_dir, repeats, filter_status),
        _benchmark_postgres(df, repeats, filter_status),
    ]

    results_df = pd.DataFrame(results)
    results_df.to_csv(output_dir / "benchmark_results.csv", index=False)

    write_partitioned_parquet(df, path_for("partition_dir"))

    return results_df
