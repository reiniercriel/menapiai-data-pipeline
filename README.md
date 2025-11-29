# Menapi.ai Data Pipeline

My playground for some data-ingestion, AI, ML and Web stuff. Please see menapiai-docs repo for an overview

Menapi.ai is an AI-powered platform that discovers emerging **business, job, and investment opportunities** by combining housing, labor market, and financial data.

This repository hosts the **data ingestion and normalization layer** for Menapi.ai.

## What this service does

  - Local housing market trends (sales, inventory, permits)
  - Public employment data (government stats)
  - Publicly available private employment data (job postings, labor intel APIs)
  - Rent and mortgage information
  - Demographics and location context (census-style data)

## Tech stack


## Quick Start

### Installation

```bash
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
pip install -e .[dev]
```

### Generate Housing Data

```bash
# Generate housing trends data for a specific city and date range
menapiai-pipeline generatehousingdata \
  --city Portland \
  --state OR \
  --start 2020-01-01 \
  --end 2024-12-31
```

**Parameters:**

**Output:**
- Partitioned dataset at `data/clean/housing_trends/`.
- Partitions: `property_type_partition=<slug>/year=<YYYY>/`.
- Wide schema with columns like `period_begin`, `median_sale_price`, `homes_sold`, `inventory`, `median_days_on_market`, `property_type`, etc.

### Example Output

```bash
✓ Successfully generated 1 dataset in data/clean/
  - data/clean/housing_trends (partitioned by property_type_partition/year)
```
### Generate Employment Data

```bash
# Generate employment trends for a metro area and date range
menapiai-pipeline generateemploymentdata \
  --metro "Portland-Vancouver-Hillsboro, OR-WA" \
  --start 2010-01-01 \
  --end 2019-12-31

# Optional: force download (ignore 24h cache)
menapiai-pipeline --log-level INFO generateemploymentdata \
  --metro "Portland-Vancouver-Hillsboro, OR-WA" \
  --start 2010-01-01 \
  --end 2019-12-31 \
  --refresh
```

**Output:**
- Partitioned dataset at `data/clean/employment_trends/`.
- Partitions: `region_partition=<slug>/year=<YYYY>/`.
- Wide schema with columns: `region_id`, `region_name`, `period`, `year`, `month`, `labor_force`, `employed`, `unemployed`, `unemployment_rate`, etc.

Notes:
- Some BLS LAUS MSA series end in 2019 due to re-delineations; use 2010–2019 for current mappings.

### Plotting: Sanity Checks

Generate quick plots for every Parquet in `data/clean/`:

```bash
python scripts/plot_all_clean_parquet.py
```

This reads partitioned datasets first (if present) and falls back to any flat Parquet files. It writes PNGs to `data/plots/`.
- Housing: plots median sale price, homes sold, inventory, and median days on market when present, grouped by property type.
- Employment: plots labor force, employed, unemployed, and unemployment rate for each region.

### Reading Partitioned Datasets

Using pandas (pyarrow engine):

```python
import pandas as pd

# Housing: filter to Single Family Residential in 2022
df_h = pd.read_parquet(
  "data/clean/housing_trends",
)
df_h = df_h[(df_h["property_type_partition"] == "single_family_residential") & (df_h["year"] == 2022)]

# Employment: filter to Portland MSA for 2015
df_e = pd.read_parquet(
  "data/clean/employment_trends",
)
df_e = df_e[(df_e["region_partition"] == "portland_vancouver_hillsboro_or_wa") & (df_e["year"] == 2015)]
```
```
