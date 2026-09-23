import pandas as pd
import pytest

from src.common.audit import record_hash
from src.transform.curated import build_curated
from src.transform.staging import _dedupe_latest
from src.validate.quality import validate_curated


def test_dedupe_latest_keeps_most_recent_updated_at():
    df = pd.DataFrame({
        "customer_id": ["C1", "C1", "C2"],
        "city": ["Old City", "New City", "Only City"],
        "updated_at": pd.to_datetime(["2024-01-01", "2025-01-01", "2024-06-01"], utc=True),
    })
    result = _dedupe_latest(df, "customer_id")
    assert len(result) == 2
    kept = result.loc[result["customer_id"] == "C1", "city"].iloc[0]
    assert kept == "New City"


def test_record_hash_ignores_keys_not_in_the_list():
    row_a = {"order_id": "O1", "quantity": 2, "processed_at_utc": "2026-01-01T00:00:00Z"}
    row_b = {"order_id": "O1", "quantity": 2, "processed_at_utc": "2026-06-01T12:00:00Z"}
    keys = ["order_id", "quantity"]
    assert record_hash(row_a, keys) == record_hash(row_b, keys)


def test_record_hash_changes_when_business_content_changes():
    row_a = {"order_id": "O1", "quantity": 2}
    row_b = {"order_id": "O1", "quantity": 3}
    keys = ["order_id", "quantity"]
    assert record_hash(row_a, keys) != record_hash(row_b, keys)


def _staging_fixture():
    customers = pd.DataFrame({
        "customer_id": ["C1"],
        "city": ["Quezon City"],
        "customer_tier": ["Gold"],
    })
    products = pd.DataFrame({
        "product_id": ["P1"],
        "name": ["Widget"],
        "category": ["Tools"],
        "brand": ["Acme"],
    })
    orders = pd.DataFrame({
        "order_id": ["O1", "O2"],
        "customer_id": ["C1", "C_UNKNOWN"],
        "product_id": ["P1", "P1"],
        "order_timestamp": pd.to_datetime(["2026-01-05", "2026-01-06"], utc=True),
        "quantity": [3, 1],
        "unit_price": [100.0, 50.0],
        "discount_pct": [0.1, 0.0],
        "status": ["DELIVERED", "PENDING"],
        "updated_at": pd.to_datetime(["2026-01-05T01:00:00Z", "2026-01-06T01:00:00Z"], utc=True),
    })
    return {"customers": customers, "products": products, "orders": orders}


def test_build_curated_computes_amounts_correctly():
    curated, _ = build_curated(_staging_fixture(), run_id="test-run")
    row = curated.loc[curated["order_id"] == "O1"].iloc[0]
    assert row["gross_amount"] == pytest.approx(300.0)
    assert row["discount_amount"] == pytest.approx(30.0)
    assert row["net_amount"] == pytest.approx(270.0)


def test_build_curated_quarantines_orphan_customer_instead_of_dropping():
    curated, quarantine = build_curated(_staging_fixture(), run_id="test-run")
    assert "O2" not in curated["order_id"].values
    assert len(quarantine) == 1
    assert quarantine.iloc[0]["reason"] == "orphan_customer_reference"


def test_validate_curated_flags_out_of_range_quantity():
    curated, _ = build_curated(_staging_fixture(), run_id="test-run")
    curated.loc[0, "quantity"] = 999
    errors = validate_curated(curated)
    assert any("quantity" in e for e in errors)


def test_validate_curated_passes_clean_data():
    curated, _ = build_curated(_staging_fixture(), run_id="test-run")
    assert validate_curated(curated) == []
