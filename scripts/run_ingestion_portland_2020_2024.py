"""
Example script: End-to-end pipeline for Portland, OR Single Family housing 2020-2024.
"""
from menapiai_data_pipeline.ingestion.housing_redfin import ingest_housing_redfin
from menapiai_data_pipeline.transforms.housing_redfin_to_canonical import transform_housing_redfin_to_canonical

if __name__ == "__main__":
    # Step 1: Download/cache raw Redfin data (no filtering)
    print("Step 1: Ingesting raw Redfin data...")
    raw_tsv_path = ingest_housing_redfin()
    print(f"Raw data at: {raw_tsv_path}")
    
    # Step 2: Transform with all filters applied
    print("\nStep 2: Transforming to canonical format...")
    canonical_paths = transform_housing_redfin_to_canonical(
        raw_tsv_path=raw_tsv_path,
        city="Portland",
        state="Oregon",
        start_date="2020-01-01",
        end_date="2024-12-31",
    )
    print(f"Created {len(canonical_paths)} canonical Parquet file(s):")
    for path in canonical_paths:
        print(f"  - {path}")
