from menapiai_data_pipeline.shared.canonical_columns_housing import (
    CANONICAL_HOUSING_REGION_ID,
    CANONICAL_HOUSING_PERIOD_BEGIN,
    CANONICAL_HOUSING_MEDIAN_SALE_PRICE,
    CANONICAL_HOUSING_HOMES_SOLD,
    CANONICAL_HOUSING_INVENTORY,
    CANONICAL_HOUSING_MEDIAN_DAYS_ON_MARKET,
)
from menapiai_data_pipeline.shared.raw_columns_housing_redfin import (
    RAW_HOUSING_REDFIN_CITY,
    RAW_HOUSING_REDFIN_STATE,
    RAW_HOUSING_REDFIN_PERIOD_BEGIN,
    RAW_HOUSING_REDFIN_MEDIAN_SALE_PRICE,
    RAW_HOUSING_REDFIN_HOMES_SOLD,
    RAW_HOUSING_REDFIN_INVENTORY,
    RAW_HOUSING_REDFIN_MEDIAN_DOM,
)
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
        CANONICAL_HOUSING_REGION_ID: df[RAW_HOUSING_REDFIN_CITY] + "_" + df[RAW_HOUSING_REDFIN_STATE],
        CANONICAL_HOUSING_PERIOD_BEGIN: pd.to_datetime(df[RAW_HOUSING_REDFIN_PERIOD_BEGIN]),
        CANONICAL_HOUSING_MEDIAN_SALE_PRICE: df[RAW_HOUSING_REDFIN_MEDIAN_SALE_PRICE],
        CANONICAL_HOUSING_HOMES_SOLD: df[RAW_HOUSING_REDFIN_HOMES_SOLD],
        CANONICAL_HOUSING_INVENTORY: df[RAW_HOUSING_REDFIN_INVENTORY],
        CANONICAL_HOUSING_MEDIAN_DAYS_ON_MARKET: df[RAW_HOUSING_REDFIN_MEDIAN_DOM],
    })

    # Save to Parquet
    clean_dir = Path(settings.processed_data_dir).parent / "clean"
    clean_dir.mkdir(parents=True, exist_ok=True)
    output_path = clean_dir / "housing_trends.parquet"
    df_canonical.to_parquet(output_path, index=False)
    logger.info(f"Saved canonical housing trends to {output_path}")

    return df_canonical
