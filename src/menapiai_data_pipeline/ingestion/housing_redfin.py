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

"""Redfin city-level housing data ingestion module."""


import pandas as pd
import requests
from pathlib import Path
from datetime import datetime, timedelta
import os

from menapiai_data_pipeline.config import settings
from menapiai_data_pipeline.logging_config import get_logger
from menapiai_data_pipeline.constants import REDFIN_CITY_URL

logger = get_logger(__name__)


def download_redfin_city_tsv(url: str, output_path: Path) -> None:
    """
    Download Redfin city-level TSV.gz from URL and save locally.
    """
    logger.info(f"Downloading Redfin TSV.gz from {url}")
    response = requests.get(url)
    response.raise_for_status()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "wb") as f:
        f.write(response.content)
    logger.info(f"Saved Redfin TSV.gz to {output_path}")



def ingest_housing_city_redfin(
    city: str = "Portland",
    state: str = "Oregon",
    local_path: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    property_type: str | None = None,
) -> str:
    """
    Ingest Redfin city-level housing data, filter by city/state, normalize schema.
    Downloads the TSV file only if not present or older than 24 hours.
    """
    # Download or use local file
    if local_path:
        tsv_path = Path(local_path)
    else:
        today = datetime.today().strftime("%Y%m%d")
        tsv_path = Path(settings.raw_data_dir) / f"redfin_housing_{today}.tsv.gz"
        # Check if file exists and is <24h old
        if tsv_path.exists():
            mtime = datetime.fromtimestamp(tsv_path.stat().st_mtime)
            if datetime.now() - mtime < timedelta(hours=24):
                logger.info(f"Using cached Redfin TSV.gz from {tsv_path}")
            else:
                logger.info(f"Cached file is older than 24h, downloading new file.")
                download_redfin_city_tsv(REDFIN_CITY_URL, tsv_path)
        else:
            download_redfin_city_tsv(REDFIN_CITY_URL, tsv_path)

    df = pd.read_csv(tsv_path, sep='\t', compression='gzip')
    logger.info(f"Loaded {len(df)} records from {tsv_path}")

    # Filter to selected city/state
    df_city = df[(df[RAW_HOUSING_REDFIN_CITY] == city) & (df[RAW_HOUSING_REDFIN_STATE] == state)]
    if property_type and RAW_HOUSING_REDFIN_PROPERTY_TYPE in df_city.columns:
        df_city = df_city[df_city[RAW_HOUSING_REDFIN_PROPERTY_TYPE] == property_type]

    # Optional filter by time span (PERIOD_BEGIN)
    if start_date or end_date:
        period = pd.to_datetime(df_city[RAW_HOUSING_REDFIN_PERIOD_BEGIN])
        if start_date:
            start = pd.to_datetime(start_date)
            df_city = df_city[period >= start]
        if end_date:
            end = pd.to_datetime(end_date)
            df_city = df_city[period <= end]

    logger.info(
        f"Filtered to {len(df_city)} records for city={city}, state={state}, "
        f"start_date={start_date}, end_date={end_date}"
    )

    # Save filtered TSV for downstream use
    safe_type = property_type.replace("/", "-").replace(" ", "_").lower() if property_type else "all"
    filtered_tsv_path = tsv_path.parent / f"housing_redfin_{city}_{state}_{safe_type}_{start_date or 'start'}_{end_date or 'end'}.tsv"
    df_city.to_csv(filtered_tsv_path, sep='\t', index=False)
    logger.info(f"Saved filtered Redfin TSV to {filtered_tsv_path}")
    return str(filtered_tsv_path)
