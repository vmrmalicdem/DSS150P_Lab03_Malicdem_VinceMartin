import pandas as pd

from src.common.audit import record_hash, utc_now_iso

CURATED_HASH_KEYS = [
    "order_id", "customer_id", "product_id", "order_timestamp",
    "customer_city", "customer_tier", "product_name", "category", "brand",
    "quantity", "unit_price", "discount_pct", "gross_amount",
    "discount_amount", "net_amount", "status", "source_updated_at",
]

CURATED_COLUMNS = CURATED_HASH_KEYS + ["pipeline_run_id", "processed_at_utc", "record_hash"]


def _orphan_reason(row) -> str:
    missing_customer = pd.isna(row["customer_city"])
    missing_product = pd.isna(row["product_name"])
    if missing_customer and missing_product:
        return "orphan_customer_and_product_reference"
    if missing_customer:
        return "orphan_customer_reference"
    return "orphan_product_reference"


def build_curated(staging: dict, run_id: str):
    orders = staging["orders"].copy()
    customers = staging["customers"][["customer_id", "city", "customer_tier"]].rename(
        columns={"city": "customer_city"}
    )
    products = staging["products"][["product_id", "name", "category", "brand"]].rename(
        columns={"name": "product_name"}
    )

    merged = orders.merge(customers, on="customer_id", how="left")
    merged = merged.merge(products, on="product_id", how="left")

    orphan = merged["customer_city"].isna() | merged["product_name"].isna()

    quarantine_frames = []
    if orphan.any():
        bad = merged.loc[orphan].copy()
        bad["reason"] = bad.apply(_orphan_reason, axis=1)
        bad["source_table"] = "curated_join"
        quarantine_frames.append(bad)

    valid = merged.loc[~orphan].copy()

    valid["quantity"] = valid["quantity"].astype(int)
    valid["unit_price"] = valid["unit_price"].astype(float)
    valid["discount_pct"] = valid["discount_pct"].astype(float)
    valid["gross_amount"] = valid["quantity"] * valid["unit_price"]
    valid["discount_amount"] = valid["gross_amount"] * valid["discount_pct"]
    valid["net_amount"] = valid["gross_amount"] - valid["discount_amount"]

    valid = valid.rename(columns={"updated_at": "source_updated_at"})
    valid["processed_at_utc"] = utc_now_iso()
    valid["pipeline_run_id"] = run_id

    valid["record_hash"] = valid.apply(
        lambda row: record_hash(row.to_dict(), CURATED_HASH_KEYS), axis=1
    )

    curated = valid[CURATED_COLUMNS].reset_index(drop=True)

    if quarantine_frames:
        quarantine = pd.concat(quarantine_frames, ignore_index=True)
    else:
        quarantine = pd.DataFrame(columns=["reason", "source_table"])

    return curated, quarantine
