"""Configuration module for the data pipeline."""

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )

    # API Settings
    api_base_url: str = "http://localhost:8000"
    api_timeout: int = 30

    # Data paths
    data_dir: str = "data"
    raw_data_dir: str = "data/raw"
    processed_data_dir: str = "data/processed"
    output_dir: str = "data/clean"  # Hardcoded output for Parquet files

    # BLS API settings
    bls_api_key: str | None = None

    # Future: Cloud database settings (reserved for later upgrades)
    # db_connection_string: str | None = None
    # cloud_storage_bucket: str | None = None

    # Feature store settings
    feature_store_url: str = "http://localhost:8001"


# Global settings instance
settings = Settings()
