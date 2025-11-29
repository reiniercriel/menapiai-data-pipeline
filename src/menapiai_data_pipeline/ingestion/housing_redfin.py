"""Redfin city-level housing data ingestion module."""

import requests
from pathlib import Path
from datetime import datetime, timedelta

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



def ingest_housing_redfin(
    local_path: str | None = None,
    force_refresh: bool = False,
) -> str:
    """
    Download and cache the complete Redfin city-level housing dataset.
    No filtering applied - returns path to raw TSV.gz for downstream transforms.
    
    Args:
        local_path: Optional path to local TSV.gz file to use instead of downloading
        force_refresh: If True, download fresh data even if cache is valid
    
    Returns:
        Path to raw Redfin TSV.gz file
    """
    # Download or use local file
    if local_path:
        tsv_path = Path(local_path)
        logger.info(f"Using local Redfin TSV.gz from {tsv_path}")
    else:
        today = datetime.today().strftime("%Y%m%d")
        tsv_path = Path(settings.raw_data_dir) / f"redfin_housing_{today}.tsv.gz"
        # Check if file exists and is <24h old (unless force_refresh)
        if tsv_path.exists() and not force_refresh:
            mtime = datetime.fromtimestamp(tsv_path.stat().st_mtime)
            if datetime.now() - mtime < timedelta(hours=24):
                logger.info(f"Using cached Redfin TSV.gz from {tsv_path}")
                return str(tsv_path)
            else:
                logger.info(f"Cached file is older than 24h, downloading new file.")
        
        download_redfin_city_tsv(REDFIN_CITY_URL, tsv_path)
    
    logger.info(f"Raw Redfin data available at {tsv_path}")
    return str(tsv_path)
