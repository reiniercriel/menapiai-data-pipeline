"""Housing data ingestion module."""

import pandas as pd
from pathlib import Path

from menapiai_data_pipeline.config import settings
from menapiai_data_pipeline.logging_config import get_logger

logger = get_logger(__name__)


def ingest_housing_data() -> pd.DataFrame:
    """
    Ingest basic housing data.

    Returns:
        DataFrame containing raw housing data
    """
    logger.info("Ingesting housing data...")

    # TODO: Implement actual data ingestion logic
    # This is a placeholder that creates sample data
    data = {
        "property_id": [1, 2, 3],
        "location": ["Location A", "Location B", "Location C"],
        "price": [250000, 350000, 450000],
        "bedrooms": [3, 4, 5],
        "bathrooms": [2, 3, 3],
    }

    df = pd.DataFrame(data)

    # Save raw data
    output_dir = Path(settings.raw_data_dir) / "housing"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "housing_basic.parquet"

    df.to_parquet(output_path, index=False)
    logger.info(f"Saved {len(df)} records to {output_path}")

    return df
