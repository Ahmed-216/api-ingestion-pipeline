import os
import pandas as pd
import pytest
import api_ingestion_pipeline.config as cfg
from api_ingestion_pipeline.utils.utils import BaseClass

# Collect all filenames across all data sources (single source of truth)
FILENAMES = [
    source_cfg["filename"]
    for provider in cfg.DATA_SOURCES.values()
    for source_cfg in provider.values()
]


@pytest.fixture(scope="module")
def prod_conn():
    base = BaseClass()
    return base.sql_prod


@pytest.mark.parametrize("filename", FILENAMES, ids=FILENAMES)
def test_raw_table_exists_in_prod(prod_conn, filename):
    """Verify each RAW table exists in the prod database."""
    # Use pandas read_sql to check if table exists
    pd.read_sql(f"SELECT COUNT(*) FROM {filename}", prod_conn)


@pytest.mark.parametrize("filename", FILENAMES, ids=FILENAMES)
def test_table_content_matches_csv(prod_conn, filename):
    """Compare DB content to source CSV with numeric tolerance per table."""
    csv_path = os.path.join(cfg.RAW_DATA_DIR, f"{filename}.csv")
    if not os.path.exists(csv_path):
        pytest.skip(f"CSV not found for {filename}: " f"{csv_path}")

    csv_df = pd.read_csv(csv_path, encoding="utf-8")
    db_df = pd.read_sql(f"SELECT * FROM {filename}", prod_conn)

    # Enforce exact schema matchs
    csv_only = sorted(list(set(csv_df.columns) - set(db_df.columns)))
    db_only = sorted(list(set(db_df.columns) - set(csv_df.columns)))
    assert set(csv_df.columns) == set(db_df.columns), (
        f"Column mismatch for {filename}:\n"
        f"CSV only: {csv_only}\n"
        f"DB only: {db_only}"
    )

    # Row count check
    assert len(csv_df) == len(db_df), (
        f"Row count mismatch for {filename}: " f"CSV={len(csv_df)} DB={len(db_df)}"
    )

    # Standardize column order and sort rows deterministically (no key dependency)
    columns = list(csv_df.columns)
    csv_sorted = csv_df[columns].sort_values(by=columns).reset_index(drop=True)
    db_sorted = db_df[columns].sort_values(by=columns).reset_index(drop=True)

    # Use same column order as CSV for both
    common_cols = list(csv_sorted.columns)
    csv_sorted = csv_sorted[common_cols]
    db_sorted = db_sorted[common_cols]

    pd.testing.assert_frame_equal(
        csv_sorted,
        db_sorted,
        check_exact=False,
        atol=1e-3,
        rtol=1e-3,
        check_dtype=False,
    )
