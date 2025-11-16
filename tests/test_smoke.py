"""Smoke tests for the data pipeline."""

import pytest


def test_import_package():
    """Test that the package can be imported."""
    import menapiai_data_pipeline
    assert menapiai_data_pipeline.__version__ == "0.1.0"


def test_import_config():
    """Test that config module can be imported."""
    from menapiai_data_pipeline.config import settings
    assert settings is not None


def test_import_logging():
    """Test that logging module can be imported."""
    from menapiai_data_pipeline.logging_config import setup_logging, get_logger
    logger = get_logger("test")
    assert logger is not None


def test_import_ingestion_modules():
    """Test that ingestion modules can be imported."""
    from menapiai_data_pipeline.ingestion import housing_basic, jobs_electrician_basic
    assert hasattr(housing_basic, "ingest_housing_data")
    assert hasattr(jobs_electrician_basic, "ingest_jobs_data")


def test_import_transform_modules():
    """Test that transform modules can be imported."""
    from menapiai_data_pipeline.transforms import housing, jobs
    assert hasattr(housing, "transform_housing_data")
    assert hasattr(jobs, "transform_jobs_data")
