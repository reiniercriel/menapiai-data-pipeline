from menapiai_data_pipeline.shared.canonical_columns_housing import (
    CANONICAL_HOUSING_REGION_ID,
    CANONICAL_HOUSING_PERIOD_BEGIN,
    CANONICAL_HOUSING_PERIOD_END,
    CANONICAL_HOUSING_MEDIAN_SALE_PRICE,
    CANONICAL_HOUSING_HOMES_SOLD,
    CANONICAL_HOUSING_INVENTORY,
    CANONICAL_HOUSING_MEDIAN_DAYS_ON_MARKET,
    CANONICAL_HOUSING_PROPERTY_TYPE,
    CANONICAL_HOUSING_PERIOD_MONTH,
    CANONICAL_HOUSING_LAST_UPDATED,
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
import re
from datetime import datetime

from menapiai_data_pipeline.config import settings
from menapiai_data_pipeline.logging_config import get_logger
from menapiai_data_pipeline.shared.regions import lookup_cbsa_from_city_state

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

    # Normalize state input (handle abbreviations)
    STATE_ABBREV_TO_NAME = {
        "AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas",
        "CA": "California", "CO": "Colorado", "CT": "Connecticut", "DE": "Delaware",
        "FL": "Florida", "GA": "Georgia", "HI": "Hawaii", "ID": "Idaho",
        "IL": "Illinois", "IN": "Indiana", "IA": "Iowa", "KS": "Kansas",
        "KY": "Kentucky", "LA": "Louisiana", "ME": "Maine", "MD": "Maryland",
        "MA": "Massachusetts", "MI": "Michigan", "MN": "Minnesota", "MS": "Mississippi",
        "MO": "Missouri", "MT": "Montana", "NE": "Nebraska", "NV": "Nevada",
        "NH": "New Hampshire", "NJ": "New Jersey", "NM": "New Mexico", "NY": "New York",
        "NC": "North Carolina", "ND": "North Dakota", "OH": "Ohio", "OK": "Oklahoma",
        "OR": "Oregon", "PA": "Pennsylvania", "RI": "Rhode Island", "SC": "South Carolina",
        "SD": "South Dakota", "TN": "Tennessee", "TX": "Texas", "UT": "Utah",
        "VT": "Vermont", "VA": "Virginia", "WA": "Washington", "WV": "West Virginia",
        "WI": "Wisconsin", "WY": "Wyoming", "DC": "District of Columbia"
    }
    state_full = STATE_ABBREV_TO_NAME.get(state.upper(), state)

    # Filter by city and state
    df = df[(df[RAW_HOUSING_REDFIN_CITY] == city) & (df[RAW_HOUSING_REDFIN_STATE] == state_full)]
    logger.info(f"Filtered to {len(df)} records for {city}, {state_full}")

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
    clean_dir = Path(settings.output_dir)
    clean_dir.mkdir(parents=True, exist_ok=True)
    
    # Prepare combined canonical dataframe across all property types
    canonical_frames = []
    for prop_type in property_types:
        df_type = df[df[RAW_HOUSING_REDFIN_PROPERTY_TYPE] == prop_type].copy()
        if df_type.empty:
            logger.warning(f"No data for property_type={prop_type}, skipping")
            continue

        # Resolve harmonized region_id (CBSA) if available; else fallback to city_state
        cbsa_code = lookup_cbsa_from_city_state(city, state_full)
        if cbsa_code:
            region_id_series = pd.Series(cbsa_code, index=df_type.index)
        else:
            region_id_series = df_type[RAW_HOUSING_REDFIN_CITY] + "_" + df_type[RAW_HOUSING_REDFIN_STATE]

        frame = pd.DataFrame({
            CANONICAL_HOUSING_REGION_ID: region_id_series,
            CANONICAL_HOUSING_PERIOD_BEGIN: pd.to_datetime(df_type[RAW_HOUSING_REDFIN_PERIOD_BEGIN]),
            CANONICAL_HOUSING_PERIOD_END: pd.to_datetime(df_type[RAW_HOUSING_REDFIN_PERIOD_END]) if RAW_HOUSING_REDFIN_PERIOD_END in df_type.columns else pd.to_datetime(df_type[RAW_HOUSING_REDFIN_PERIOD_BEGIN]),
            CANONICAL_HOUSING_MEDIAN_SALE_PRICE: df_type[RAW_HOUSING_REDFIN_MEDIAN_SALE_PRICE],
            CANONICAL_HOUSING_HOMES_SOLD: df_type[RAW_HOUSING_REDFIN_HOMES_SOLD],
            CANONICAL_HOUSING_INVENTORY: df_type[RAW_HOUSING_REDFIN_INVENTORY],
            CANONICAL_HOUSING_MEDIAN_DAYS_ON_MARKET: df_type[RAW_HOUSING_REDFIN_MEDIAN_DOM],
            CANONICAL_HOUSING_PROPERTY_TYPE: prop_type,
        })
        # Derived monthly key and metadata
        frame[CANONICAL_HOUSING_PERIOD_MONTH] = frame[CANONICAL_HOUSING_PERIOD_BEGIN].dt.to_period("M").dt.to_timestamp()
        frame[CANONICAL_HOUSING_LAST_UPDATED] = datetime.now().isoformat()
        # Ensure chronological order within each type
        frame = frame.sort_values(by=[CANONICAL_HOUSING_PERIOD_BEGIN]).reset_index(drop=True)
        canonical_frames.append(frame)

        # (Legacy single-file outputs removed; using partitioned dataset only)

    # Write partitioned dataset layout: data/clean/housing_trends/property_type_partition=.../year=...
    if canonical_frames:
        df_all = pd.concat(canonical_frames, ignore_index=True)
        # Add partitions
        df_all["year"] = df_all[CANONICAL_HOUSING_PERIOD_BEGIN].dt.year

        def slugify(val: str) -> str:
            s = str(val).lower()
            s = s.replace("/", "-")
            s = re.sub(r"[^a-z0-9]+", "_", s)
            return s.strip("_")

        df_all["property_type_partition"] = df_all[CANONICAL_HOUSING_PROPERTY_TYPE].map(slugify)

        # Normalize dtypes to avoid Arrow dictionary unification issues
        for col in [
            CANONICAL_HOUSING_REGION_ID,
            CANONICAL_HOUSING_PROPERTY_TYPE,
            CANONICAL_HOUSING_LAST_UPDATED,
            "property_type_partition",
        ]:
            if col in df_all.columns:
                df_all[col] = df_all[col].astype("string")

        dataset_path = clean_dir / "housing_trends"
        # Save as partitioned dataset (requires pyarrow)
        df_all.to_parquet(
            dataset_path,
            index=False,
            partition_cols=["property_type_partition", "year"],
            engine="pyarrow",
            use_dictionary=False,
        )
        output_paths = [str(dataset_path)]
        logger.info(f"Saved partitioned dataset at {dataset_path} (partitioned by property_type_partition/year)")
    
    logger.info(f"Transform complete. Created {len(output_paths)} Parquet file(s)")
    return output_paths
