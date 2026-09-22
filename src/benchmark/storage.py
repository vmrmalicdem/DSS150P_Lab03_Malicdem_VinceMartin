import statistics
import time
from pathlib import Path

import pandas as pd


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
    start = time.perf_counter()
    df.to_csv(path, index=False)
    write_seconds = time.perf_counter() - start

    full_seconds, full_df = _median_time(lambda: pd.read_csv(path), repeats)
    filtered_seconds, filtered_df = _median_time(
        lambda: pd.read_csv(path).loc[lambda d: d["status"] == filter_status], repeats
    )
    return {
        "storage_type": "csv", "file_size_bytes": path.stat().st_size,
        "write_seconds": write_seconds, "full_read_seconds": full_seconds,
        "filtered_read_seconds": filtered_seconds, "row_count": len(full_df),
        "notes": f"filtered rows={len(filtered_df)}",
    }


def _benchmark_jsonl(df, formats_dir, repeats, filter_status):
    path = formats_dir / "sales_order_lines.jsonl"
    start = time.perf_counter()
    df.to_json(path, orient="records", lines=True, date_format="iso")
    write_seconds = time.perf_counter() - start

    full_seconds, full_df = _median_time(lambda: pd.read_json(path, lines=True), repeats)
    filtered_seconds, filtered_df = _median_time(
        lambda: pd.read_json(path, lines=True).loc[lambda d: d["status"] == filter_status], repeats
    )
    return {
        "storage_type": "json_lines", "file_size_bytes": path.stat().st_size,
        "write_seconds": write_seconds, "full_read_seconds": full_seconds,
        "filtered_read_seconds": filtered_seconds, "row_count": len(full_df),
        "notes": f"filtered rows={len(filtered_df)}",
    }


def _benchmark_parquet(df, formats_dir, repeats, filter_status):
    path = formats_dir / "sales_order_lines.parquet"
    start = time.perf_counter()
    df.to_parquet(path, index=False, compression="snappy")
    write_seconds = time.perf_counter() - start

    full_seconds, full_df = _median_time(lambda: pd.read_parquet(path), repeats)
    filtered_seconds, filtered_df = _median_time(
        lambda: pd.read_parquet(path, filters=[("status", "==", filter_status)]), repeats
    )
    return {
        "storage_type": "parquet_snappy", "file_size_bytes": path.stat().st_size,
        "write_seconds": write_seconds, "full_read_seconds": full_seconds,
        "filtered_read_seconds": filtered_seconds, "row_count": len(full_df),
        "notes": f"filtered rows={len(filtered_df)}",
    }


def _benchmark_postgres(repeats, filter_status):
    try:
        from src.load.postgres import _connect
        with _connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT pg_total_relation_size(%s)", ("curated.sales_order_lines",))
                size_bytes = cur.fetchone()[0]

                def full_query():
                    cur.execute("SELECT * FROM curated.sales_order_lines")
                    return cur.fetchall()

                def filtered_query():
                    cur.execute(
                        "SELECT * FROM curated.sales_order_lines WHERE status = %s",
                        (filter_status,),
                    )
                    return cur.fetchall()

                full_seconds, full_rows = _median_time(full_query, repeats)
                filtered_seconds, filtered_rows = _median_time(filtered_query, repeats)
        return {
            "storage_type": "postgresql", "file_size_bytes": size_bytes,
            "write_seconds": None, "full_read_seconds": full_seconds,
            "filtered_read_seconds": filtered_seconds, "row_count": len(full_rows),
            "notes": f"size via pg_total_relation_size; filtered rows={len(filtered_rows)}; write time not comparable here, see load command timing",
        }
    except Exception as exc:
        return {
            "storage_type": "postgresql", "file_size_bytes": None,
            "write_seconds": None, "full_read_seconds": None,
            "filtered_read_seconds": None, "row_count": None,
            "notes": f"postgresql unavailable: {exc}",
        }


def write_partitioned_parquet(df, output_dir):
    df = df.copy()
    order_ts = pd.to_datetime(df["order_timestamp"], utc=True)
    df["order_year"] = order_ts.dt.year
    df["order_month"] = order_ts.dt.month
    output_dir = Path(output_dir)
    df.to_parquet(output_dir, index=False, partition_cols=["order_year", "order_month"], compression="snappy")
    return output_dir


def run_benchmark(curated_path, output_dir, repeats: int = 5):
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
        _benchmark_postgres(repeats, filter_status),
    ]

    results_df = pd.DataFrame(results)
    results_df.to_csv(output_dir / "benchmark_results.csv", index=False)

    write_partitioned_parquet(df, path_for("partition_dir"))

    return results_df
