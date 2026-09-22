import pandas as pd

from src.config import SETTINGS


def validate_curated(df: pd.DataFrame) -> list[str]:
    """Return a list of human-readable validation errors (empty = passed)."""
    quality = SETTINGS["quality"]
    errors = []

    if df["order_id"].isna().any():
        errors.append("order_id contains null values")

    dup_count = int(df["order_id"].duplicated().sum())
    if dup_count:
        errors.append(f"order_id has {dup_count} duplicate values")

    bad_qty = ~df["quantity"].between(quality["min_quantity"], quality["max_quantity"])
    if bad_qty.any():
        errors.append(
            f"{int(bad_qty.sum())} rows have quantity outside "
            f"[{quality['min_quantity']}, {quality['max_quantity']}]"
        )

    for col in ("gross_amount", "discount_amount", "net_amount"):
        negative = df[col] < 0
        if negative.any():
            errors.append(f"{int(negative.sum())} rows have negative {col}")

    bad_status = ~df["status"].isin(quality["allowed_order_statuses"])
    if bad_status.any():
        errors.append(f"{int(bad_status.sum())} rows have a status outside the allowed set")

    for col in ("pipeline_run_id", "processed_at_utc", "record_hash", "source_updated_at"):
        missing = int(df[col].isna().sum())
        if missing:
            errors.append(f"{missing} rows missing required audit field {col}")

    return errors
