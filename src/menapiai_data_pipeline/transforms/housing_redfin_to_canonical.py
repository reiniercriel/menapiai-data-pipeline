from menapiai_data_pipeline.shared.canonical_columns_housing import (
    CANONICAL_HOUSING_REGION_ID,
    CANONICAL_HOUSING_PERIOD_BEGIN,
    CANONICAL_HOUSING_PERIOD_END,
    CANONICAL_HOUSING_MEDIAN_SALE_PRICE,
    CANONICAL_HOUSING_HOMES_SOLD,
    CANONICAL_HOUSING_INVENTORY,
    CANONICAL_HOUSING_MEDIAN_DAYS_ON_MARKET,
    CANONICAL_HOUSING_PROPERTY_TYPE,
)
from menapiai_data_pipeline.shared.raw_columns_housing_redfin import (
    RAW_HOUSING_REDFIN_CITY,
    RAW_HOUSING_REDFIN_STATE,
    RAW_HOUSING_REDFIN_PERIOD_BEGIN,
    RAW_HOUSING_REDFIN_PERIOD_END,
    RAW_HOUSING_REDFIN_MEDIAN_SALE_PRICE,
    RAW_HOUSING_REDFIN_HOMES_SOLD,
    RAW_HOUSING_REDFIN_INVENTORY,
    RAW_HOUSING_REDFIN_MEDIAN_DOM,
    RAW_HOUSING_REDFIN_PROPERTY_TYPE,
)
"""Transform Redfin housing CSV to canonical housing_trends table."""

import pandas as pd
from pathlib import Path

from menapiai_data_pipeline.config import settings
from menapiai_data_pipeline.logging_config import get_logger

logger = get_logger(__name__)


def transform_housing_redfin_to_canonical(raw_tsv_path: str, property_type: str) -> str:
    """
    Transform raw Redfin CSV to canonical housing_trends Parquet.
    """
    raw_tsv_path = Path(raw_tsv_path)
    if not raw_tsv_path.exists():
        logger.error(f"Raw Redfin TSV not found at {raw_tsv_path}")
        raise FileNotFoundError(f"Raw Redfin TSV not found at {raw_tsv_path}")

    df = pd.read_csv(raw_tsv_path, sep='\t')
    logger.info(f"Loaded {len(df)} records from {raw_tsv_path}")

    # Map to canonical schema (including property type)
    df_canonical = pd.DataFrame({
        CANONICAL_HOUSING_REGION_ID: df[RAW_HOUSING_REDFIN_CITY] + "_" + df[RAW_HOUSING_REDFIN_STATE],
        CANONICAL_HOUSING_PERIOD_BEGIN: pd.to_datetime(df[RAW_HOUSING_REDFIN_PERIOD_BEGIN]),
        CANONICAL_HOUSING_PERIOD_END: pd.to_datetime(df[RAW_HOUSING_REDFIN_PERIOD_END]) if RAW_HOUSING_REDFIN_PERIOD_END in df.columns else pd.to_datetime(df[RAW_HOUSING_REDFIN_PERIOD_BEGIN]),
        CANONICAL_HOUSING_MEDIAN_SALE_PRICE: df[RAW_HOUSING_REDFIN_MEDIAN_SALE_PRICE],
        CANONICAL_HOUSING_HOMES_SOLD: df[RAW_HOUSING_REDFIN_HOMES_SOLD],
        CANONICAL_HOUSING_INVENTORY: df[RAW_HOUSING_REDFIN_INVENTORY],
        CANONICAL_HOUSING_MEDIAN_DAYS_ON_MARKET: df[RAW_HOUSING_REDFIN_MEDIAN_DOM],
        CANONICAL_HOUSING_PROPERTY_TYPE: df[RAW_HOUSING_REDFIN_PROPERTY_TYPE] if RAW_HOUSING_REDFIN_PROPERTY_TYPE in df.columns else property_type,
    })

    # Filter for the requested property type only
    df_canonical = df_canonical[df_canonical[CANONICAL_HOUSING_PROPERTY_TYPE] == property_type]
    if df_canonical.empty:
        raise ValueError(f"No data for property_type={property_type} in canonical transform.")

    # Save one Parquet file for this property type, adding a data_end_date column
    clean_dir = Path(settings.processed_data_dir).parent / "clean"
    clean_dir.mkdir(parents=True, exist_ok=True)
    safe_type = property_type.replace("/", "-").replace(" ", "_").lower()
    start_date = df_canonical[CANONICAL_HOUSING_PERIOD_BEGIN].min()
    end_date = df_canonical[CANONICAL_HOUSING_PERIOD_END].max()
    df_canonical = df_canonical.copy()
    df_canonical["data_start_date"] = start_date
    df_canonical["data_end_date"] = end_date
    output_path = clean_dir / f"housing_trends_{safe_type}.parquet"
    df_canonical.to_parquet(output_path, index=False)
    logger.info(f"Saved canonical housing trends for property_type={property_type} to {output_path} (start_date={start_date.date()}, end_date={end_date.date()})")
    return str(output_path)
