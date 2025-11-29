"""Command-line interface for the data pipeline."""

import click

from menapiai_data_pipeline.logging_config import setup_logging, get_logger

logger = get_logger(__name__)


@click.group()
@click.option("--log-level", default="INFO", help="Logging level")
def main(log_level: str) -> None:
    """MENAPI AI Data Pipeline CLI."""
    setup_logging(log_level)

@main.command()
@click.option("--city", type=str, required=True, help="City name (e.g., Portland)")
@click.option("--state", type=str, required=True, help="State name or abbreviation (e.g., OR, Oregon)")
@click.option("--start", type=str, required=True, help="Start date (flexible formats: 01012020, 01/01/2020, 2020-01-01)")
@click.option("--end", type=str, required=True, help="End date (flexible formats: 12312024, 12/31/2024, 2024-12-31)")
def GenerateHousingData(city: str, state: str, start: str, end: str) -> None:
    """
    Generate housing data Parquet files for a specific city, state, and date range.
    Downloads raw data, transforms to canonical format, and outputs to data/clean.
    """
    from menapiai_data_pipeline.ingestion.housing_redfin import ingest_housing_redfin
    from menapiai_data_pipeline.transforms.housing_redfin_to_canonical import transform_housing_redfin_to_canonical
    from dateutil import parser as date_parser
    
    # Parse flexible date formats to YYYY-MM-DD
    try:
        # Handle common formats: MMDDYYYY, MM/DD/YYYY, YYYY-MM-DD
        start_clean = start.replace('/', '-')
        end_clean = end.replace('/', '-')
        
        # Try parsing with dateutil (handles most formats)
        start_date = date_parser.parse(start_clean, dayfirst=False).strftime("%Y-%m-%d")
        end_date = date_parser.parse(end_clean, dayfirst=False).strftime("%Y-%m-%d")
    except Exception as e:
        logger.error(f"Invalid date format: {e}")
        raise click.BadParameter(
            f"Could not parse dates. Examples: 2020-01-01, 01/01/2020, 2020/01/01\"\n"
            f"Received: start='{start}', end='{end}'\""
        )
    
    logger.info(f"Generating housing data for {city}, {state} from {start_date} to {end_date}")
    
    # Step 1: Ingest raw data
    logger.info("Step 1/2: Downloading raw Redfin data...")
    raw_tsv_path = ingest_housing_redfin()
    
    # Step 2: Transform to canonical Parquet files (one per property type)
    logger.info("Step 2/2: Transforming to canonical format...")
    output_paths = transform_housing_redfin_to_canonical(
        raw_tsv_path=raw_tsv_path,
        city=city,
        state=state,
        start_date=start_date,
        end_date=end_date,
    )
    
    logger.info(f"✓ Successfully generated {len(output_paths)} Parquet file(s) in data/clean/")
    for path in output_paths:
        logger.info(f"  - {path}")


@main.command()
@click.option("--metro", type=str, required=True, help="Metro area name (e.g., 'Portland-Vancouver-Hillsboro, OR-WA')")
@click.option("--start", type=str, required=True, help="Start date (flexible formats: 01012020, 01/01/2020, 2020-01-01)")
@click.option("--end", type=str, required=True, help="End date (flexible formats: 12312024, 12/31/2024, 2024-12-31)")
@click.option("--refresh/--no-refresh", default=False, help="Force refresh of raw BLS data (ignore 24h cache)")
def GenerateEmploymentData(metro: str, start: str, end: str, refresh: bool) -> None:
    """
    Generate employment data Parquet file for a specific metro area and date range.
    Downloads raw BLS LAUS data, transforms to canonical format, and outputs to data/clean.
    """
    from menapiai_data_pipeline.ingestion.employment_bls import ingest_employment_bls
    from menapiai_data_pipeline.transforms.employment_bls_to_canonical import transform_employment_bls_to_canonical
    from menapiai_data_pipeline.constants import BLS_METRO_AREAS
    from dateutil import parser as date_parser
    
    # Validate metro area
    if metro not in BLS_METRO_AREAS:
        available = "\n  ".join(BLS_METRO_AREAS.keys())
        logger.error(f"Unknown metro area: {metro}")
        raise click.BadParameter(
            f"Metro area '{metro}' not configured.\n"
            f"Available metro areas:\n  {available}"
        )
    
    # Parse flexible date formats to YYYY-MM-DD
    try:
        start_clean = start.replace('/', '-')
        end_clean = end.replace('/', '-')
        
        start_date = date_parser.parse(start_clean, dayfirst=False).strftime("%Y-%m-%d")
        end_date = date_parser.parse(end_clean, dayfirst=False).strftime("%Y-%m-%d")
    except Exception as e:
        logger.error(f"Invalid date format: {e}")
        raise click.BadParameter(
            f"Could not parse dates. Examples: 2020-01-01, 01/01/2020, 2020/01/01\n"
            f"Received: start='{start}', end='{end}'"
        )
    
    logger.info(f"Generating employment data for {metro} from {start_date} to {end_date}")
    
    # Step 1: Ingest raw data
    logger.info("Step 1/2: Downloading raw BLS LAUS data...")
    raw_json_path = ingest_employment_bls(force_refresh=refresh)
    
    # Step 2: Transform to canonical Parquet file
    logger.info("Step 2/2: Transforming to canonical format...")
    output_paths = transform_employment_bls_to_canonical(
        raw_json_path=raw_json_path,
        metro_area=metro,
        start_date=start_date,
        end_date=end_date,
    )
    
    logger.info(
        f"✓ Successfully generated {len(output_paths)} employment Parquet file(s) in data/clean/"
    )
    for path in output_paths:
        logger.info(f"  - {path}")


if __name__ == "__main__":
    main()
