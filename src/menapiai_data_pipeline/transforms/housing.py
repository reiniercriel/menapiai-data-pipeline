"""Housing data transformation module."""

import pandas as pd
from pathlib import Path

from menapiai_data_pipeline.config import settings
from menapiai_data_pipeline.logging_config import get_logger

logger = get_logger(__name__)


def transform_housing_data() -> pd.DataFrame:
    """
    Transform housing data for feature engineering.

    Returns:
        DataFrame containing transformed housing data
    """
    logger.info("Transforming housing data...")

    # Load raw data
    input_path = Path(settings.raw_data_dir) / "housing" / "housing_basic.parquet"

    if not input_path.exists():
        logger.error(f"Raw data not found at {input_path}")
        raise FileNotFoundError(f"Raw data not found at {input_path}")

    df = pd.read_parquet(input_path)
    logger.info(f"Loaded {len(df)} records from {input_path}")

    # TODO: Implement actual transformation logic
    # Example transformations:
    df["price_per_bedroom"] = df["price"] / df["bedrooms"]
    df["total_rooms"] = df["bedrooms"] + df["bathrooms"]

    # Save processed data
    output_dir = Path(settings.processed_data_dir) / "housing"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "housing_transformed.parquet"

    df.to_parquet(output_path, index=False)
    logger.info(f"Saved {len(df)} transformed records to {output_path}")

    return df
