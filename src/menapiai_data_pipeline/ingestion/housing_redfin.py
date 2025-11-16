"""Redfin housing data ingestion module."""

import pandas as pd
import requests
from pathlib import Path
from datetime import datetime

from menapiai_data_pipeline.config import settings
from menapiai_data_pipeline.logging_config import get_logger

logger = get_logger(__name__)

REDFIN_CITY_URL = (
    "https://redfin-public-data.s3.us-west-2.amazonaws.com/"
    "redfin_market_tracker/city_market_tracker.tsv000.gz"
)


# Example city filter: Portland, OR
CITY_FILTERS = [
    {"city": "Portland", "state": "Oregon"},
]


def download_redfin_csv(url: str, output_path: Path) -> None:
    """
    Download Redfin CSV from URL and save locally.
    """
    logger.info(f"Downloading Redfin TSV.gz from {url}")
    response = requests.get(url)
    response.raise_for_status()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "wb") as f:
        f.write(response.content)
    logger.info(f"Saved Redfin TSV.gz to {output_path}")


def ingest_housing_redfin(local_path: str = None) -> pd.DataFrame:
    """
    Ingest Redfin housing data, filter regions, normalize schema.
    """
    # Download or use local file
    if local_path:
        tsv_path = Path(local_path)
    else:
        today = datetime.today().strftime("%Y%m%d")
        tsv_path = Path(settings.raw_data_dir) / f"redfin_housing_{today}.tsv.gz"
        download_redfin_csv(REDFIN_CITY_URL, tsv_path)

    df = pd.read_csv(tsv_path, sep='\t', compression='gzip')
    logger.info(f"Loaded {len(df)} records from {tsv_path}")


    # Filter to selected cities
    mask = False
    for cf in CITY_FILTERS:
        mask |= ((df["CITY"] == cf["city"]) & (df["STATE"] == cf["state"]))
    df = df[mask]
    logger.info(f"Filtered to {len(df)} records for selected cities")

    # Normalize to canonical schema
    df_canonical = pd.DataFrame({
        "region_id": df["CITY"] + "_" + df["STATE"],
        "period_begin": pd.to_datetime(df["PERIOD_BEGIN"]),
        "median_sale_price": df["MEDIAN_SALE_PRICE"],
        "homes_sold": df["HOMES_SOLD"],
        "inventory": df["INVENTORY"],
        "median_days_on_market": df["MEDIAN_DOM"],
    })

    # Save raw filtered TSV
    raw_tsv_path = Path(settings.raw_data_dir) / "housing_redfin.tsv"
    df.to_csv(raw_tsv_path, sep='\t', index=False)
    logger.info(f"Saved filtered Redfin TSV to {raw_tsv_path}")

    return df_canonical
