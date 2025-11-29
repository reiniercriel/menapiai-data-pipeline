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

# Keep transform consistent with ingestion property type aliases
PROPERTY_TYPE_ALIASES = {
    "single family": "Single Family Residential",
    "single family residential": "Single Family Residential",
    "condo": "Condo/Co-op",
    "condo/co-op": "Condo/Co-op",
    "townhouse": "Townhouse",
    "multi-family": "Multi-Family (2-4 Unit)",
    "multi-family (2-4 unit)": "Multi-Family (2-4 Unit)",
    "all": "All Residential",
    "all residential": "All Residential",
}


def transform_housing_redfin_to_canonical(
    raw_tsv_path: str,
    city: str,
    state: str,
    start_date: str,
    end_date: str,
) -> list[str]:
    """
    Transform raw Redfin TSV to canonical housing_trends Parquet files.
    Applies filtering by city, state, and date range.
    Creates one Parquet file per property type found in the filtered data.
    
    Args:
        raw_tsv_path: Path to raw Redfin TSV.gz file
        city: City name to filter
        state: State name to filter
        start_date: Start date (YYYY-MM-DD) for PERIOD_BEGIN filter
        end_date: End date (YYYY-MM-DD) for PERIOD_BEGIN filter
    
    Returns:
        List of paths to canonical Parquet files (one per property type)
    """
    raw_tsv_path = Path(raw_tsv_path)
    if not raw_tsv_path.exists():
        logger.error(f"Raw Redfin TSV not found at {raw_tsv_path}")
        raise FileNotFoundError(f"Raw Redfin TSV not found at {raw_tsv_path}")

    df = pd.read_csv(raw_tsv_path, sep='\t', compression='gzip')
    logger.info(f"Loaded {len(df)} records from {raw_tsv_path}")

    # Filter by city and state
    df = df[(df[RAW_HOUSING_REDFIN_CITY] == city) & (df[RAW_HOUSING_REDFIN_STATE] == state)]
    logger.info(f"Filtered to {len(df)} records for {city}, {state}")

    # Validate and apply date range filters
    try:
        start = pd.to_datetime(start_date)
        end = pd.to_datetime(end_date)
    except Exception as e:
        raise ValueError(f"Invalid date format for start_date/end_date: {e}")

    if start > end:
        raise ValueError("start_date must be on or before end_date")

    period_begin = pd.to_datetime(df[RAW_HOUSING_REDFIN_PERIOD_BEGIN])
    df = df[(period_begin >= start) & (period_begin <= end)].copy()
    
    # If PERIOD_END exists, ensure the row's period window intersects the requested window
    if RAW_HOUSING_REDFIN_PERIOD_END in df.columns:
        period_begin_filtered = pd.to_datetime(df[RAW_HOUSING_REDFIN_PERIOD_BEGIN])
        period_end_filtered = pd.to_datetime(df[RAW_HOUSING_REDFIN_PERIOD_END])
        df = df[(period_end_filtered >= start) & (period_begin_filtered <= end)].copy()
    
    logger.info(f"Filtered to {len(df)} records for date range {start_date} to {end_date}")
    
    if df.empty:
        raise ValueError(
            f"No data found for city={city}, state={state}, "
            f"date range {start_date} to {end_date}"
        )

    # Get unique property types in the filtered data
    if RAW_HOUSING_REDFIN_PROPERTY_TYPE not in df.columns:
        raise ValueError(f"Raw data missing {RAW_HOUSING_REDFIN_PROPERTY_TYPE} column")
    
    property_types = df[RAW_HOUSING_REDFIN_PROPERTY_TYPE].dropna().unique()
    logger.info(f"Found {len(property_types)} property types: {sorted(property_types.tolist())}")
    
    output_paths = []
    clean_dir = Path(settings.processed_data_dir).parent / "clean"
    clean_dir.mkdir(parents=True, exist_ok=True)
    
    # Process each property type separately
    for prop_type in property_types:
        df_type = df[df[RAW_HOUSING_REDFIN_PROPERTY_TYPE] == prop_type].copy()
        
        if df_type.empty:
            logger.warning(f"No data for property_type={prop_type}, skipping")
            continue

        # Map to canonical schema for this property type
        df_canonical = pd.DataFrame({
            CANONICAL_HOUSING_REGION_ID: df_type[RAW_HOUSING_REDFIN_CITY] + "_" + df_type[RAW_HOUSING_REDFIN_STATE],
            CANONICAL_HOUSING_PERIOD_BEGIN: pd.to_datetime(df_type[RAW_HOUSING_REDFIN_PERIOD_BEGIN]),
            CANONICAL_HOUSING_PERIOD_END: pd.to_datetime(df_type[RAW_HOUSING_REDFIN_PERIOD_END]) if RAW_HOUSING_REDFIN_PERIOD_END in df_type.columns else pd.to_datetime(df_type[RAW_HOUSING_REDFIN_PERIOD_BEGIN]),
            CANONICAL_HOUSING_MEDIAN_SALE_PRICE: df_type[RAW_HOUSING_REDFIN_MEDIAN_SALE_PRICE],
            CANONICAL_HOUSING_HOMES_SOLD: df_type[RAW_HOUSING_REDFIN_HOMES_SOLD],
            CANONICAL_HOUSING_INVENTORY: df_type[RAW_HOUSING_REDFIN_INVENTORY],
            CANONICAL_HOUSING_MEDIAN_DAYS_ON_MARKET: df_type[RAW_HOUSING_REDFIN_MEDIAN_DOM],
            CANONICAL_HOUSING_PROPERTY_TYPE: prop_type,
        })
        
        # Add metadata columns
        data_start = df_canonical[CANONICAL_HOUSING_PERIOD_BEGIN].min()
        data_end = df_canonical[CANONICAL_HOUSING_PERIOD_END].max()
        df_canonical["data_start_date"] = data_start
        df_canonical["data_end_date"] = data_end
        
        # Generate filename with dates and normalized property type
        safe_type = prop_type.replace("/", "-").replace(" ", "_").lower()
        output_path = clean_dir / f"housing_trends_{safe_type}_{start_date}_{end_date}.parquet"
        df_canonical.to_parquet(output_path, index=False)
        output_paths.append(str(output_path))
        logger.info(
            f"Saved {len(df_canonical)} records for property_type={prop_type} to {output_path} "
            f"(data_start={data_start.date()}, data_end={data_end.date()})"
        )
    
    logger.info(f"Transform complete. Created {len(output_paths)} Parquet file(s)")
    return output_paths
