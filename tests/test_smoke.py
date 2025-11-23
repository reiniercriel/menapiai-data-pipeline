"""Smoke tests for the data pipeline."""

import pytest


def test_import_package():
    """Test that the package can be imported."""
    import menapiai_data_pipeline

    assert menapiai_data_pipeline.__version__ == "0.1.0"


def test_import_ingestion_modules():
    """Test that ingestion modules can be imported."""
    from menapiai_data_pipeline.ingestion import ingest_housing_city_redfin

    assert callable(ingest_housing_city_redfin)


def test_full_pipeline_portland_oregon():
    """Full smoke test: ingest and transform Portland, OR data."""
    from pathlib import Path

    from menapiai_data_pipeline.config import settings
    from menapiai_data_pipeline.ingestion import ingest_housing_city_redfin
    from menapiai_data_pipeline.transforms.housing_redfin_to_canonical import (
        transform_housing_redfin_to_canonical,
    )

    # Run ingestion for Portland, OR and write raw TSV (full span)
    df_ingested = ingest_housing_city_redfin(city="Portland", state="Oregon")

    assert not df_ingested.empty
    assert (df_ingested["region_id"] == "Portland_Oregon").any()

    # Ensure raw TSV exists where the transform expects it
    raw_tsv_path = Path(settings.raw_data_dir) / "housing_redfin.tsv"
    assert raw_tsv_path.exists()

    # Run transform to canonical Parquet for the full span
    df_canonical = transform_housing_redfin_to_canonical()

    assert not df_canonical.empty
    assert (df_canonical["region_id"] == "Portland_Oregon").any()

    # Now run ingestion with a bounded time span as an extra smoke check
    df_ingested_bounded = ingest_housing_city_redfin(
        city="Portland",
        state="Oregon",
        start_date="2020-01-01",
        end_date="2020-12-31",
    )

    # It may legitimately be empty for that range, but it must not error
    assert df_ingested_bounded is not None
