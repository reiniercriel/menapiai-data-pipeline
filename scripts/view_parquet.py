"""
Helper script to view Parquet files in the MENAPI AI data pipeline.
"""

import sys
import pandas as pd

if len(sys.argv) < 2:
    print("Usage: python view_parquet.py <path_to_parquet_file> [--head N]")
    sys.exit(1)

parquet_path = sys.argv[1]
head_n = 5
if len(sys.argv) == 4 and sys.argv[2] == "--head":
    head_n = int(sys.argv[3])

try:
    df = pd.read_parquet(parquet_path)
    print(f"Loaded {len(df)} rows from {parquet_path}\n")
    print(df.head(head_n))
except Exception as e:
    print(f"Error reading Parquet file: {e}")
    sys.exit(2)
