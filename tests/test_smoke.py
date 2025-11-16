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
