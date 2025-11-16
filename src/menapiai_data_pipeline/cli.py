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
@click.option("--source", type=click.Choice(["housing", "jobs"]), required=True)
def ingest(source: str) -> None:
    """
    Ingest data from a source.

    Args:
        source: Data source to ingest (housing or jobs)
    """
    logger.info(f"Starting ingestion for source: {source}")

    if source == "housing":
        from menapiai_data_pipeline.ingestion.housing_basic import ingest_housing_data
        ingest_housing_data()
    elif source == "jobs":
        from menapiai_data_pipeline.ingestion.jobs_electrician_basic import ingest_jobs_data
        ingest_jobs_data()

    
@main.command()
@click.option("--local-path", type=str, default=None, help="Path to local Redfin CSV (optional)")
def ingest_housing_redfin(local_path: str) -> None:
    """
    Ingest Redfin housing data (metro-level).
    """
    logger.info("Starting Redfin housing ingestion...")
    from menapiai_data_pipeline.ingestion.housing_redfin import ingest_housing_redfin
    ingest_housing_redfin(local_path)
    logger.info("Completed Redfin housing ingestion.")

@main.command()
def transform_housing_redfin() -> None:
    """
    Transform Redfin housing data to canonical housing_trends table.
    """
    logger.info("Starting Redfin housing transformation...")
    from menapiai_data_pipeline.transforms.housing_redfin_to_canonical import transform_housing_redfin_to_canonical
    transform_housing_redfin_to_canonical()
    logger.info("Completed Redfin housing transformation.")


@main.command()
@click.option("--dataset", type=click.Choice(["housing", "jobs"]), required=True)
def transform(dataset: str) -> None:
    """
    Transform a dataset.

    Args:
        dataset: Dataset to transform (housing or jobs)
    """
    logger.info(f"Starting transformation for dataset: {dataset}")

    if dataset == "housing":
        from menapiai_data_pipeline.transforms.housing import transform_housing_data
        transform_housing_data()
    elif dataset == "jobs":
        from menapiai_data_pipeline.transforms.jobs import transform_jobs_data
        transform_jobs_data()

    logger.info(f"Completed transformation for dataset: {dataset}")


if __name__ == "__main__":
    main()
