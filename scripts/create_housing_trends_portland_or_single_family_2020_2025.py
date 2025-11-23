import pandas as pd
from pathlib import Path
from menapiai_data_pipeline.config import settings

def main():
    # 1. Ingest and transform for Portland, OR, 2020, single family
    from menapiai_data_pipeline.ingestion import ingest_housing_city_redfin
    from menapiai_data_pipeline.transforms.housing_redfin_to_canonical import transform_housing_redfin_to_canonical


    # Ingest for Portland, OR, 2020-2025 (will filter by city/state and year)
    property_type = "Single Family Residential"
    raw_tsv = ingest_housing_city_redfin(
        city="Portland",
        state="Oregon",
        start_date="2020-01-01",
        end_date="2025-10-31",
        property_type=property_type,
    )
    # Transform to canonical (writes only one property type Parquet file)
    parquet_path = transform_housing_redfin_to_canonical(raw_tsv, property_type)

    output_path = Path(settings.processed_data_dir).parent / "clean" / "housing_trends_portland_or_single_family_2020_2025.parquet"

    df = pd.read_parquet(parquet_path)
    if df.empty:
        raise ValueError(f"No data for parquet file: {parquet_path}")

    # Group by month and compute median sale price
    df["month"] = df["period_begin"].dt.to_period("M").dt.to_timestamp()
    monthly = df.groupby("month")["median_sale_price"].median().reset_index()

    # Save to Parquet
    monthly.to_parquet(output_path, index=False)
    print(f"Wrote {len(monthly)} rows to {output_path}")

if __name__ == "__main__":
    main()
