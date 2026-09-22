import argparse
import os
from pathlib import Path

import pandas as pd

from src.config import PROJECT_ROOT, DB, SETTINGS, path_for
from src.common.audit import new_run_id, utc_now_iso
from src.extract.files import extract_sources
from src.transform.staging import build_staging
from src.transform.curated import build_curated
from src.validate.quality import validate_curated
from src.benchmark.storage import run_benchmark, write_partitioned_parquet


def current_run_id() -> str:
    run_id = os.getenv("PIPELINE_RUN_ID")
    if run_id:
        return run_id
    run_id = new_run_id()
    os.environ["PIPELINE_RUN_ID"] = run_id
    return run_id


def latest_raw_dir() -> Path:
    raw_dir = path_for("raw_dir")
    pointer = raw_dir / "_latest_run_id.txt"
    if not pointer.exists():
        raise FileNotFoundError("No raw snapshot found. Run \"extract\" first.")
    return raw_dir / f"run_id={pointer.read_text(encoding='utf-8').strip()}"


def do_extract(run_id):
    raw_dir = extract_sources(run_id)
    print(f"Extracted sources into {raw_dir}")
    return raw_dir


def do_transform(run_id):
    raw_dir = latest_raw_dir()
    staging, staging_q = build_staging(raw_dir, run_id)

    staging_dir = path_for("staging_dir")
    staging_dir.mkdir(parents=True, exist_ok=True)
    for name, frame in staging.items():
        frame.to_parquet(staging_dir / f"{name}.parquet", index=False)

    curated, curated_q = build_curated(staging, run_id)
    curated_dir = path_for("curated_dir")
    curated_dir.mkdir(parents=True, exist_ok=True)
    curated.to_parquet(curated_dir / "sales_order_lines.parquet", index=False)

    quarantine = pd.concat([staging_q, curated_q], ignore_index=True)
    quarantine_dir = path_for("quarantine_dir")
    quarantine_dir.mkdir(parents=True, exist_ok=True)
    quarantine.to_csv(quarantine_dir / f"quarantine_{run_id}.csv", index=False)

    rows_staging = sum(len(f) for f in staging.values())
    print(f"Staging rows: {rows_staging}")
    print(f"Curated rows: {len(curated)}")
    print(f"Quarantined rows: {len(quarantine)}")
    return staging, curated, quarantine


def do_load(run_id):
    from src.load.postgres import upsert_curated
    curated_path = path_for("curated_dir") / "sales_order_lines.parquet"
    curated = pd.read_parquet(curated_path)
    affected = upsert_curated(curated, run_id)
    print(f"Upserted {affected} rows into curated.sales_order_lines")
    return affected


def do_validate():
    curated_path = path_for("curated_dir") / "sales_order_lines.parquet"
    curated = pd.read_parquet(curated_path)
    errors = validate_curated(curated)
    if errors:
        print("VALIDATION FAILED:")
        for err in errors:
            print(f" - {err}")
        raise SystemExit(1)
    print(f"Validation passed for {len(curated)} curated rows.")


def main():
    parser = argparse.ArgumentParser(description="DSS150P modular pipeline")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("validate-env")
    sub.add_parser("extract")
    sub.add_parser("transform")
    sub.add_parser("load")
    sub.add_parser("validate")
    b = sub.add_parser("benchmark"); b.add_argument("--repeats", type=int, default=5)
    p = sub.add_parser("load-partition"); p.add_argument("--year", type=int, required=True); p.add_argument("--month", type=int, required=True)
    sub.add_parser("run-all")
    args = parser.parse_args()

    if args.command == "validate-env":
        print("PROJECT_ROOT=", PROJECT_ROOT)
        print("DB host/database=", DB["host"], DB["dbname"])
        print("Configured source=", SETTINGS["pipeline"]["source_dir"])
        return

    if args.command == "extract":
        do_extract(current_run_id())
        return

    if args.command == "transform":
        do_transform(current_run_id())
        return

    if args.command == "validate":
        do_validate()
        return

    if args.command == "load":
        do_load(current_run_id())
        return

    if args.command == "benchmark":
        benchmark_dir = path_for("benchmark_dir")
        benchmark_dir.mkdir(parents=True, exist_ok=True)
        curated_path = path_for("curated_dir") / "sales_order_lines.parquet"
        results = run_benchmark(curated_path, benchmark_dir, repeats=args.repeats)
        print(results.to_string(index=False))
        return

    if args.command == "load-partition":
        from src.load.postgres import load_partition
        run_id = current_run_id()
        curated_path = path_for("curated_dir") / "sales_order_lines.parquet"
        curated = pd.read_parquet(curated_path)
        order_ts = pd.to_datetime(curated["order_timestamp"], utc=True)
        curated["order_year"] = order_ts.dt.year
        curated["order_month"] = order_ts.dt.month

        partition_dir = path_for("partition_dir")
        if not partition_dir.exists():
            write_partitioned_parquet(curated.drop(columns=["order_year", "order_month"]), partition_dir)

        partition_df = pd.read_parquet(
            partition_dir,
            filters=[("order_year", "==", args.year), ("order_month", "==", args.month)],
        )
        if partition_df.empty:
            print(f"No rows found for partition {args.year}-{args.month:02d}")
            return
        rows = load_partition(partition_df, args.year, args.month, run_id)
        print(f"Loaded {rows} rows for partition {args.year}-{args.month:02d}")
        return

    if args.command == "run-all":
        from src.load.postgres import upsert_pipeline_run
        run_id = current_run_id()
        started = utc_now_iso()
        try:
            do_extract(run_id)
            staging, curated, quarantine = do_transform(run_id)
            do_load(run_id)
            do_validate()
        except Exception as exc:
            try:
                upsert_pipeline_run(run_id, status="FAILED", message=str(exc), started_at=started)
            except Exception as audit_exc:
                print(f"Warning: could not record failed run: {audit_exc}")
            raise
        else:
            rows_staging = sum(len(f) for f in staging.values())
            try:
                upsert_pipeline_run(
                    run_id, status="SUCCESS",
                    rows_staging=rows_staging, rows_curated=len(curated),
                    rows_quarantined=len(quarantine),
                    started_at=started, completed_at=utc_now_iso(), message="ok",
                )
            except Exception as exc:
                print(f"Warning: could not record pipeline_runs audit row: {exc}")
        return


if __name__ == "__main__":
    main()
