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
@click.option("--local-path", type=str, default=None, help="Path to local Redfin TSV.gz (optional)")
@click.option("--force-refresh", is_flag=True, help="Force download even if cache is valid")
def ingest_housing_redfin(local_path: str, force_refresh: bool) -> None:
    """
    Download and cache the complete Redfin housing dataset.
    No filtering applied - returns raw data for transforms.
    """
    logger.info("Starting Redfin housing data ingestion...")
    from menapiai_data_pipeline.ingestion.housing_redfin import ingest_housing_redfin
    output_path = ingest_housing_redfin(local_path=local_path, force_refresh=force_refresh)
    logger.info(f"Completed ingestion. Raw data at: {output_path}")

@main.command()
@click.option("--raw-tsv-path", type=str, required=True, help="Path to raw Redfin TSV.gz file")
@click.option("--city", type=str, required=True, help="City name to filter")
@click.option("--state", type=str, required=True, help="State name to filter")
@click.option("--start-date", type=str, required=True, help="Start date (YYYY-MM-DD)")
@click.option("--end-date", type=str, required=True, help="End date (YYYY-MM-DD)")
def transform_housing_redfin(
    raw_tsv_path: str,
    city: str,
    state: str,
    start_date: str,
    end_date: str,
) -> None:
    """
    Transform Redfin housing data to canonical housing_trends Parquet files.
    Applies filtering by city, state, and date range.
    Creates one Parquet file per property type.
    """
    logger.info(
        f"Starting Redfin transformation for {city}, {state}, "
        f"{start_date} to {end_date}..."
    )
    from menapiai_data_pipeline.transforms.housing_redfin_to_canonical import transform_housing_redfin_to_canonical
    output_paths = transform_housing_redfin_to_canonical(
        raw_tsv_path=raw_tsv_path,
        city=city,
        state=state,
        start_date=start_date,
        end_date=end_date,
    )
    logger.info(f"Completed transformation. Created {len(output_paths)} file(s):")
    for path in output_paths:
        logger.info(f"  - {path}")

if __name__ == "__main__":
    main()
