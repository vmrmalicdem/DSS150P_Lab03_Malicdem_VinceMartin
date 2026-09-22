import json
from pathlib import Path

import pandas as pd

from src.common.audit import utc_now_iso
from src.config import SETTINGS


def _dedupe_latest(df: pd.DataFrame, key: str, updated_col: str = 'updated_at') -> pd.DataFrame:
    """Keep the most recent row per business key based on updated_at."""
    df = df.sort_values(updated_col)
    return df.drop_duplicates(subset=key, keep='last').reset_index(drop=True)


def _load_customers(raw_dir: Path) -> pd.DataFrame:
    df = pd.read_csv(raw_dir / 'customers.csv')
    df['created_at'] = pd.to_datetime(df['created_at'], utc=True, errors='coerce')
    df['updated_at'] = pd.to_datetime(df['updated_at'], utc=True, errors='coerce')
    return df


def _load_products(raw_dir: Path) -> pd.DataFrame:
    with (raw_dir / 'products.json').open(encoding='utf-8') as f:
        records = json.load(f)
    df = pd.json_normalize(records)
    df = df.rename(columns={'category.name': 'category', 'category.department': 'department'})
    df['updated_at'] = pd.to_datetime(df['updated_at'], utc=True, errors='coerce')
    return df


def _load_orders(raw_dir: Path) -> pd.DataFrame:
    df = pd.read_csv(raw_dir / 'orders.csv')
    df['order_timestamp'] = pd.to_datetime(df['order_timestamp'], utc=True, errors='coerce')
    df['updated_at'] = pd.to_datetime(df['updated_at'], utc=True, errors='coerce')
    return df


def build_staging(raw_dir: Path, run_id: str):
    """Create cleaned, typed staging datasets.

    Returns (staging: dict[str, DataFrame], quarantine: DataFrame).
    """
    quality = SETTINGS['quality']
    staged_at = utc_now_iso()
    quarantine_frames = []

    # ---------------------------------------------------------------- customers
    customers = _load_customers(raw_dir)
    customers = _dedupe_latest(customers, 'customer_id')
    customers['email'] = customers['email'].astype('string').str.strip().str.lower()
    customers['email_missing'] = customers['email'].isna() | (customers['email'] == '')
    customers['city'] = customers['city'].astype('string').str.strip().str.title()
    customers['pipeline_run_id'] = run_id
    customers['staged_at_utc'] = staged_at

    # ----------------------------------------------------------------- products
    products = _load_products(raw_dir)
    products = _dedupe_latest(products, 'product_id')
    products['unit_price'] = pd.to_numeric(products['unit_price'], errors='coerce')
    invalid_price = products['unit_price'].isna() | (products['unit_price'] <= 0)
    if invalid_price.any():
        bad = products.loc[invalid_price].copy()
        bad['reason'] = 'invalid_or_negative_price'
        bad['source_table'] = 'products'
        quarantine_frames.append(bad)
    products = products.loc[~invalid_price].reset_index(drop=True)
    products['pipeline_run_id'] = run_id
    products['staged_at_utc'] = staged_at

    # ------------------------------------------------------------------- orders
    orders = _load_orders(raw_dir)
    orders = _dedupe_latest(orders, 'order_id')
    orders['quantity'] = pd.to_numeric(orders['quantity'], errors='coerce')

    reasons = pd.Series([None] * len(orders), index=orders.index, dtype='object')

    bad_qty = (
        orders['quantity'].isna()
        | (orders['quantity'] < quality['min_quantity'])
        | (orders['quantity'] > quality['max_quantity'])
    )
    reasons.loc[bad_qty] = 'invalid_quantity'

    bad_status = ~orders['status'].isin(quality['allowed_order_statuses'])
    reasons.loc[bad_status & reasons.isna()] = 'invalid_status'

    bad_ts = orders['order_timestamp'].isna() | orders['updated_at'].isna()
    reasons.loc[bad_ts & reasons.isna()] = 'invalid_timestamp'

    is_bad = reasons.notna()
    if is_bad.any():
        bad = orders.loc[is_bad].copy()
        bad['reason'] = reasons.loc[is_bad]
        bad['source_table'] = 'orders'
        quarantine_frames.append(bad)
    orders = orders.loc[~is_bad].reset_index(drop=True)
    orders['pipeline_run_id'] = run_id
    orders['staged_at_utc'] = staged_at

    if quarantine_frames:
        quarantine = pd.concat(quarantine_frames, ignore_index=True)
    else:
        quarantine = pd.DataFrame(columns=['reason', 'source_table'])

    staging = {'customers': customers, 'products': products, 'orders': orders}
    return staging, quarantine