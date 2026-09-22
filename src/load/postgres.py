from datetime import datetime, timezone

import psycopg

from src.config import DB

CURATED_COLUMNS = [
    "order_id", "customer_id", "product_id", "order_timestamp",
    "customer_city", "customer_tier", "product_name", "category", "brand",
    "quantity", "unit_price", "discount_pct", "gross_amount",
    "discount_amount", "net_amount", "status", "source_updated_at",
    "pipeline_run_id", "processed_at_utc", "record_hash",
]


def _connect():
    return psycopg.connect(
        host=DB["host"], port=DB["port"], dbname=DB["dbname"],
        user=DB["user"], password=DB["password"],
    )


def upsert_curated(df, run_id: str) -> int:
    if df.empty:
        return 0
    cols = CURATED_COLUMNS
    placeholders = ", ".join(f"%({c})s" for c in cols)
    col_list = ", ".join(cols)
    update_cols = [c for c in cols if c != "order_id"]
    update_clause = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)
    sql = f"""
        INSERT INTO curated.sales_order_lines ({col_list})
        VALUES ({placeholders})
        ON CONFLICT (order_id) DO UPDATE SET {update_clause}
        WHERE curated.sales_order_lines.record_hash <> EXCLUDED.record_hash
    """
    records = df[cols].to_dict(orient="records")
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.executemany(sql, records)
        conn.commit()
    return len(records)


def load_partition(df, year: int, month: int, run_id: str) -> int:
    subset = df[CURATED_COLUMNS]
    rows_loaded = upsert_curated(subset, run_id)
    partition_key = f"{year:04d}-{month:02d}"
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO audit.partition_loads (partition_key, loaded_at_utc, row_count, pipeline_run_id)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (partition_key) DO UPDATE SET
                    loaded_at_utc = EXCLUDED.loaded_at_utc,
                    row_count = EXCLUDED.row_count,
                    pipeline_run_id = EXCLUDED.pipeline_run_id
                """,
                (partition_key, datetime.now(timezone.utc), rows_loaded, run_id),
            )
        conn.commit()
    return rows_loaded
