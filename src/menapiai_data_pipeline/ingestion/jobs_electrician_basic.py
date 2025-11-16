"""Jobs (electrician) data ingestion module."""

import pandas as pd
from pathlib import Path

from menapiai_data_pipeline.config import settings
from menapiai_data_pipeline.logging_config import get_logger

logger = get_logger(__name__)


def ingest_jobs_data() -> pd.DataFrame:
    """
    Ingest basic electrician jobs data.

    Returns:
        DataFrame containing raw jobs data
    """
    logger.info("Ingesting electrician jobs data...")

    # TODO: Implement actual data ingestion logic
    # This is a placeholder that creates sample data
    data = {
        "job_id": [1, 2, 3],
        "title": ["Electrician - Residential", "Master Electrician", "Apprentice Electrician"],
        "company": ["Company A", "Company B", "Company C"],
        "location": ["City A", "City B", "City C"],
        "salary": [55000, 75000, 35000],
    }

    df = pd.DataFrame(data)

    # Save raw data
    output_dir = Path(settings.raw_data_dir) / "jobs"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "jobs_electrician_basic.parquet"

    df.to_parquet(output_path, index=False)
    logger.info(f"Saved {len(df)} records to {output_path}")

    return df
