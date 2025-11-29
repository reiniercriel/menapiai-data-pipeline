"""Transform BLS employment JSON to canonical employment table."""

import json
import pandas as pd
from pathlib import Path
from datetime import datetime
import re

from menapiai_data_pipeline.config import settings
from menapiai_data_pipeline.logging_config import get_logger
from menapiai_data_pipeline.constants import BLS_METRO_AREAS
from menapiai_data_pipeline.shared.canonical_columns_employment import (
    REGION_ID,
    REGION_NAME,
    REGION_TYPE,
    PERIOD,
    PERIOD_MONTH,
    YEAR,
    MONTH,
    LABOR_FORCE,
    EMPLOYED,
    UNEMPLOYED,
    UNEMPLOYMENT_RATE,
    DATA_SOURCE,
    LAST_UPDATED,
)
from menapiai_data_pipeline.shared.raw_columns_employment_bls import (
    STATUS,
    MESSAGE,
    RESULTS,
    SERIES,
    SERIES_ID,
    DATA,
    YEAR as RAW_YEAR,
    PERIOD as RAW_PERIOD,
    VALUE,
)

logger = get_logger(__name__)


# BLS measure code mapping
MEASURE_CODE_TO_FIELD = {
    "03": UNEMPLOYMENT_RATE,
    "04": UNEMPLOYED,
    "05": EMPLOYED,
    "06": LABOR_FORCE,
}


def _parse_series_id(series_id: str) -> dict[str, str]:
    """
    Parse BLS LAUS series ID to extract components.
    
    Format: LAUMT + STATE_FIPS (2) + AREA_CODE (5) + EXTRA_ZEROS (6) + MEASURE (2)
    Example: LAUMT411840000000003
    
    Returns:
        Dict with 'metro_code' (7 digits) and 'measure_code' (2 digits)
    """
    # Remove LAUMT prefix
    if not series_id.startswith("LAUMT"):
        raise ValueError(f"Invalid series ID format: {series_id}")
    
    code_part = series_id[5:]  # Everything after "LAUMT"
    
    # Extract: 7 digits metro code + 6 zeros + 2 digit measure
    metro_code = code_part[:7]
    measure_code = code_part[-2:]
    
    return {
        "metro_code": metro_code,
        "measure_code": measure_code,
    }


def _find_metro_name(metro_code: str) -> str | None:
    """Find metro area name from metro code."""
    for metro_name, metro_info in BLS_METRO_AREAS.items():
        if metro_info["full_code"] == metro_code:
            return metro_name
    return None


def _parse_period(period_str: str) -> int:
    """
    Parse BLS period string to month number.
    
    Format: M01-M12 for monthly data
    
    Returns:
        Month number (1-12)
    """
    if not period_str.startswith("M"):
        raise ValueError(f"Invalid period format: {period_str}")
    return int(period_str[1:])


def transform_employment_bls_to_canonical(
    raw_json_path: str,
    metro_area: str,
    start_date: str,
    end_date: str,
) -> list[str]:
    """
    Transform raw BLS JSON to canonical employment Parquet file.
    Filters by metro area and date range.
    
    Args:
        raw_json_path: Path to raw BLS JSON file
        metro_area: Metro area name (must match key in BLS_METRO_AREAS)
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
    
    Returns:
        List of paths to canonical Parquet files (one per employment measure)
    """
    raw_json_path = Path(raw_json_path)
    if not raw_json_path.exists():
        raise FileNotFoundError(f"Raw BLS JSON not found at {raw_json_path}")
    
    # Validate metro area
    if metro_area not in BLS_METRO_AREAS:
        available = list(BLS_METRO_AREAS.keys())
        raise ValueError(
            f"Metro area '{metro_area}' not found. Available: {available}"
        )
    
    metro_code = BLS_METRO_AREAS[metro_area]["full_code"]
    
    # Parse dates
    try:
        start = pd.to_datetime(start_date)
        end = pd.to_datetime(end_date)
    except Exception as e:
        raise ValueError(f"Invalid date format: {e}")
    
    if start > end:
        raise ValueError("start_date must be on or before end_date")
    
    # Load raw JSON
    with open(raw_json_path, "r", encoding="utf-8") as f:
        response = json.load(f)
    
    # Validate API response
    if response.get(STATUS) != "REQUEST_SUCCEEDED":
        error_msg = response.get(MESSAGE, ["Unknown error"])[0]
        raise ValueError(f"BLS API request failed: {error_msg}")
    
    results = response.get(RESULTS, {})
    series_list = results.get(SERIES, [])
    
    if not series_list:
        raise ValueError("No series data in BLS response")
    
    logger.info(f"Processing {len(series_list)} series from BLS response")
    
    # Parse all series data into a structured format
    # We'll pivot from long format (one row per measure per month) to wide format
    records = []
    
    for series in series_list:
        series_id = series.get(SERIES_ID)
        data_points = series.get(DATA, [])
        
        if not series_id or not data_points:
            continue
        
        try:
            parsed_id = _parse_series_id(series_id)
        except ValueError as e:
            logger.warning(f"Skipping series {series_id}: {e}")
            continue
        
        # Only process data for the requested metro area
        if parsed_id["metro_code"] != metro_code:
            continue
        
        measure_code = parsed_id["measure_code"]
        field_name = MEASURE_CODE_TO_FIELD.get(measure_code)
        
        if not field_name:
            logger.warning(f"Unknown measure code {measure_code} in {series_id}")
            continue
        
        # Extract data points
        for dp in data_points:
            year = int(dp[RAW_YEAR])
            period_str = dp[RAW_PERIOD]
            
            # Skip annual averages
            if not period_str.startswith("M"):
                continue
            
            month = _parse_period(period_str)
            value_str = dp[VALUE]
            
            # Parse value (may have commas for thousands)
            try:
                value = float(value_str.replace(",", ""))
            except (ValueError, AttributeError):
                logger.warning(f"Invalid value '{value_str}' for {series_id} {year}-{month}")
                continue
            
            # Create period datetime
            period_dt = pd.to_datetime(f"{year}-{month:02d}-01")
            
            # Apply date filter
            if period_dt < start or period_dt > end:
                continue
            
            # Find or create record for this year-month
            record = next(
                (r for r in records if r[YEAR] == year and r[MONTH] == month),
                None
            )
            
            if record is None:
                record = {
                    YEAR: year,
                    MONTH: month,
                    PERIOD: f"{year}-{month:02d}",
                }
                records.append(record)
            
            # Add measure value to record
            record[field_name] = value
    
    if not records:
        raise ValueError(
            f"No data found for metro_area={metro_area}, "
            f"date range {start_date} to {end_date}"
        )
    
    # Convert to DataFrame
    df = pd.DataFrame(records)
    
    # Add metadata columns
    df[REGION_ID] = metro_code
    df[REGION_NAME] = metro_area
    df[REGION_TYPE] = "metro"
    df[DATA_SOURCE] = "BLS LAUS"
    df[LAST_UPDATED] = datetime.now().isoformat()
    
    # Ensure all expected measure columns exist
    for field in [LABOR_FORCE, EMPLOYED, UNEMPLOYED, UNEMPLOYMENT_RATE]:
        if field not in df.columns:
            df[field] = None
    
    # Add derived monthly key for alignment
    # Build from PERIOD to avoid int coercion issues
    df[PERIOD_MONTH] = pd.to_datetime(df[PERIOD] + "-01")

    # Reorder columns for canonical schema
    column_order = [
        REGION_ID,
        REGION_NAME,
        REGION_TYPE,
        PERIOD,
        PERIOD_MONTH,
        YEAR,
        MONTH,
        LABOR_FORCE,
        EMPLOYED,
        UNEMPLOYED,
        UNEMPLOYMENT_RATE,
        DATA_SOURCE,
        LAST_UPDATED,
    ]
    df = df[column_order]
    
    # Sort by period
    df = df.sort_values(by=[YEAR, MONTH])
    
    # Save partitioned wide dataset only: data/clean/employment_trends/region_partition=<slug>/year=<YYYY>/
    clean_dir = Path(settings.output_dir)
    clean_dir.mkdir(parents=True, exist_ok=True)
    dataset_dir = clean_dir / "employment_trends"

    def slugify(val: str) -> str:
        s = str(val).lower()
        s = s.replace(",", "")
        s = s.replace("-", "_")
        s = re.sub(r"[^a-z0-9_ ]+", "", s)
        s = s.replace(" ", "_")
        return s.strip("_")

    wide_df = df.copy()
    # Normalize string dtypes for stability across partitions
    for col in [REGION_ID, REGION_NAME, REGION_TYPE, DATA_SOURCE, LAST_UPDATED, "region_partition"]:
        if col in wide_df.columns:
            wide_df[col] = wide_df[col].astype("string")
    wide_df["region_partition"] = slugify(metro_area)

    # Save as partitioned dataset (requires pyarrow)
    dataset_dir.mkdir(parents=True, exist_ok=True)
    wide_df.to_parquet(
        dataset_dir,
        index=False,
        partition_cols=["region_partition", YEAR],
        engine="pyarrow",
        use_dictionary=False,
    )
    logger.info(
        f"Saved partitioned employment dataset at {dataset_dir} partitioned by region_partition/year"
    )
    return [str(dataset_dir)]
