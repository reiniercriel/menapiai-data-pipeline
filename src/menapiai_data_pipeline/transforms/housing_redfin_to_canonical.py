"""Transform Redfin housing CSV to canonical housing_trends table."""

import pandas as pd
from pathlib import Path

from menapiai_data_pipeline.config import settings
from menapiai_data_pipeline.logging_config import get_logger

logger = get_logger(__name__)


def transform_housing_redfin_to_canonical() -> pd.DataFrame:
    """
    Transform raw Redfin CSV to canonical housing_trends Parquet.
    """
    raw_tsv_path = Path(settings.raw_data_dir) / "housing_redfin.tsv"
    if not raw_tsv_path.exists():
        logger.error(f"Raw Redfin TSV not found at {raw_tsv_path}")
        raise FileNotFoundError(f"Raw Redfin TSV not found at {raw_tsv_path}")

    df = pd.read_csv(raw_tsv_path, sep='\t')
    logger.info(f"Loaded {len(df)} records from {raw_tsv_path}")

    # Map to canonical schema
    df_canonical = pd.DataFrame({
        "region_id": df["CITY"] + "_" + df["STATE"],
        "period_begin": pd.to_datetime(df["PERIOD_BEGIN"]),
        "median_sale_price": df["MEDIAN_SALE_PRICE"],
        "homes_sold": df["HOMES_SOLD"],
        "inventory": df["INVENTORY"],
        "median_days_on_market": df["MEDIAN_DOM"],
    })

    # Save to Parquet
    clean_dir = Path(settings.processed_data_dir).parent / "clean"
    clean_dir.mkdir(parents=True, exist_ok=True)
    output_path = clean_dir / "housing_trends.parquet"
    df_canonical.to_parquet(output_path, index=False)
    logger.info(f"Saved canonical housing trends to {output_path}")

    return df_canonical
