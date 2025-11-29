"""BLS (Bureau of Labor Statistics) employment data ingestion module."""

import requests
import json
from pathlib import Path
from datetime import datetime, timedelta

from menapiai_data_pipeline.config import settings
from menapiai_data_pipeline.logging_config import get_logger
from menapiai_data_pipeline.constants import BLS_API_BASE_URL, BLS_METRO_AREAS

logger = get_logger(__name__)


def _build_series_ids(metro_area_code: str) -> list[str]:
    """
    Build BLS LAUS series IDs for all employment measures for a metro area.
    
    Series ID format: LAUMT + STATE_FIPS + AREA_CODE + MEASURE_CODE
    
    Measure codes:
        03 = unemployment rate
        04 = unemployment
        05 = employment
        06 = labor force
    
    Args:
        metro_area_code: 7-digit metro area code (e.g., "4118400" for Portland)
    
    Returns:
        List of 4 series IDs (one for each measure)
    """
    measure_codes = ["03", "04", "05", "06"]
    return [f"LAUMT{metro_area_code}000000{code}" for code in measure_codes]


def download_bls_employment_data(
    start_year: int,
    end_year: int,
    output_path: Path,
) -> None:
    """
    Download BLS LAUS employment data for all configured metro areas.
    
    Args:
        start_year: Start year for data request
        end_year: End year for data request
        output_path: Path to save raw JSON response
    """
    # Collect all series IDs from all metro areas
    all_series_ids = []
    for metro_name, metro_info in BLS_METRO_AREAS.items():
        series_ids = _build_series_ids(metro_info["full_code"])
        all_series_ids.extend(series_ids)
        logger.info(f"Added {len(series_ids)} series for {metro_name}")
    
    # Build API request payload
    payload = {
        "seriesid": all_series_ids,
        "startyear": str(start_year),
        "endyear": str(end_year),
        "catalog": True,  # Include metadata
        "calculations": False,  # Don't include calculated fields
        "annualaverage": False,  # Monthly data only
    }
    
    # Add API key if available (increases rate limits and historical data access)
    if settings.bls_api_key:
        payload["registrationkey"] = settings.bls_api_key
        logger.info("Using BLS API key for enhanced access")
    else:
        logger.warning("No BLS API key configured - limited to 10 years of data and lower rate limits")
    
    # Make API request
    logger.info(f"Requesting BLS data for {len(all_series_ids)} series from {start_year} to {end_year}")
    headers = {"Content-type": "application/json"}
    response = requests.post(BLS_API_BASE_URL, json=payload, headers=headers)
    response.raise_for_status()
    
    # Save raw JSON response
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(response.json(), f, indent=2)
    
    logger.info(f"Saved BLS raw data to {output_path}")


def ingest_employment_bls(
    local_path: str | None = None,
    force_refresh: bool = False,
) -> str:
    """
    Download and cache complete BLS LAUS employment data for all metro areas.
    No filtering applied - returns path to raw JSON for downstream transforms.
    
    Downloads data for all years from 2000 to current year to build comprehensive
    historical dataset. With API key, can request up to 20 years per call.
    
    Args:
        local_path: Optional path to local JSON file to use instead of downloading
        force_refresh: If True, download fresh data even if cache is valid
    
    Returns:
        Path to raw BLS JSON file
    """
    if local_path:
        json_path = Path(local_path)
        logger.info(f"Using local BLS JSON from {json_path}")
        return str(json_path)
    
    # Calculate year range for download
    current_year = datetime.now().year
    start_year = 2000  # BLS LAUS data available from 1976, but 2000+ is most relevant
    
    today = datetime.today().strftime("%Y%m%d")
    json_path = Path(settings.raw_data_dir) / f"bls_employment_{today}.json"
    
    # Check if file exists and is <24h old (unless force_refresh)
    if json_path.exists() and not force_refresh:
        mtime = datetime.fromtimestamp(json_path.stat().st_mtime)
        if datetime.now() - mtime < timedelta(hours=24):
            logger.info(f"Using cached BLS JSON from {json_path}")
            return str(json_path)
        else:
            logger.info("Cached file is older than 24h, downloading new file")
    
    # Download fresh data
    download_bls_employment_data(start_year, current_year, json_path)
    
    logger.info(f"Raw BLS employment data available at {json_path}")
    return str(json_path)
