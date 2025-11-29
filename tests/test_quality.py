import os
from pathlib import Path

import pandas as pd
import pytest


CLEAN_DIR = Path("data/clean")


@pytest.mark.skipif(not (CLEAN_DIR / "housing_trends").exists(), reason="housing dataset not present")
def test_housing_schema_and_keys():
    ds = CLEAN_DIR / "housing_trends"
    # Read partition files individually to avoid pyarrow dictionary-unify issues
    parts = sorted(ds.rglob("*.parquet"))
    assert parts, "No parquet files found in housing_trends dataset"
    frames = [pd.read_parquet(p) for p in parts]
    df = pd.concat(frames, ignore_index=True)

    expected_cols = {
        "region_id",
        "period_begin",
        "period_end",
        "median_sale_price",
        "homes_sold",
        "inventory",
        "median_days_on_market",
        "property_type",
        "period_month",
        "last_updated",
    }
    missing = expected_cols - set(df.columns)
    assert not missing, f"Missing columns in housing dataset: {missing}"

    # Non-null key fields
    assert df["region_id"].notna().all(), "region_id contains nulls"
    assert df["period_month"].notna().all(), "period_month contains nulls"


@pytest.mark.skipif(not (CLEAN_DIR / "employment_trends").exists(), reason="employment dataset not present")
def test_employment_schema_and_keys():
    ds = CLEAN_DIR / "employment_trends"
    df = pd.read_parquet(ds)

    expected_cols = {
        "region_id",
        "region_name",
        "region_type",
        "period",
        "period_month",
        "year",
        "month",
        "labor_force",
        "employed",
        "unemployed",
        "unemployment_rate",
        "data_source",
        "last_updated",
    }
    missing = expected_cols - set(df.columns)
    assert not missing, f"Missing columns in employment dataset: {missing}"

    # Non-null key fields
    assert df["region_id"].notna().all(), "region_id contains nulls"
    assert df["period_month"].notna().all(), "period_month contains nulls"
