"""Jobs data transformation module."""

import pandas as pd
from pathlib import Path

from menapiai_data_pipeline.config import settings
from menapiai_data_pipeline.logging_config import get_logger

logger = get_logger(__name__)


def transform_jobs_data() -> pd.DataFrame:
    """
    Transform jobs data for feature engineering.

    Returns:
        DataFrame containing transformed jobs data
    """
    logger.info("Transforming jobs data...")

    # Load raw data
    input_path = Path(settings.raw_data_dir) / "jobs" / "jobs_electrician_basic.parquet"

    if not input_path.exists():
        logger.error(f"Raw data not found at {input_path}")
        raise FileNotFoundError(f"Raw data not found at {input_path}")

    df = pd.read_parquet(input_path)
    logger.info(f"Loaded {len(df)} records from {input_path}")

    # TODO: Implement actual transformation logic
    # Example transformations:
    df["title_lower"] = df["title"].str.lower()
    df["is_senior"] = df["title"].str.contains("Master|Senior", case=False, regex=True)

    # Save processed data
    output_dir = Path(settings.processed_data_dir) / "jobs"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "jobs_transformed.parquet"

    df.to_parquet(output_path, index=False)
    logger.info(f"Saved {len(df)} transformed records to {output_path}")

    return df
