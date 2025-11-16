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
@click.option("--city", type=str, required=True, help="City name")
@click.option("--state", type=str, required=True, help="State name")
@click.option("--local-path", type=str, default=None, help="Path to local Redfin TSV (optional)")
def ingest_housing_city_redfin(city: str, state: str, local_path: str) -> None:
    """
    Ingest Redfin city-level housing data for a specific city/state.
    """
    logger.info(f"Starting Redfin city housing ingestion for {city}, {state}...")
    from menapiai_data_pipeline.ingestion.housing_city_redfin import ingest_housing_city_redfin
    ingest_housing_city_redfin(city=city, state=state, local_path=local_path)
    logger.info("Completed Redfin city housing ingestion.")

@main.command()
def transform_housing_redfin() -> None:
    """
    Transform Redfin housing data to canonical housing_trends table.
    """
    logger.info("Starting Redfin housing transformation...")
    from menapiai_data_pipeline.transforms.housing_redfin_to_canonical import transform_housing_redfin_to_canonical
    transform_housing_redfin_to_canonical()
    logger.info("Completed Redfin housing transformation.")

if __name__ == "__main__":
    main()
