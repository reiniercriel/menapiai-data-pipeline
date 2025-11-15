# Menapi.ai Data Pipeline

Menapi.ai is an AI-powered platform that discovers emerging **business, job, and investment opportunities** by combining housing, labor market, and financial data.

This repository hosts the **data ingestion and normalization layer** for Menapi.ai.

## What this service does

- Connects to public and commercial data sources:
  - Local housing market trends (sales, inventory, permits)
  - Public employment data (government stats)
  - Publicly available private employment data (job postings, labor intel APIs)
  - Rent and mortgage information
  - Demographics and location context (census-style data)
- Cleans and normalizes data into a consistent schema
- Stores processed data into a warehouse / data lake (e.g. Postgres, BigQuery, DuckDB, etc.)
- Exposes simple CLI/cron entry points for scheduled ingestion jobs

## Tech stack

- Python 3.x
- Requests / httpx for API access
- Pandas / Polars for data wrangling
