import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path
import sys
import argparse

from menapiai_data_pipeline.config import settings

def main():
    parser = argparse.ArgumentParser(description="Plot median sale price from a specific Parquet file.")
    parser.add_argument("--parquet-file", type=str, required=True, help="Path to the Parquet file to plot.")
    args = parser.parse_args()

    parquet_path = Path(args.parquet_file)
    if not parquet_path.exists():
        raise FileNotFoundError(f"Parquet file not found: {parquet_path}")

    df = pd.read_parquet(parquet_path)
    # No region_id column, plot all data
    x_col = "month" if "month" in df.columns else df.columns[0]

    df = df.sort_values(x_col)

    # Get data_start_date and data_end_date from the file (should be the same for all rows)
    data_start_date = df["data_start_date"].iloc[0] if "data_start_date" in df.columns else df[x_col].min()
    data_end_date = df["data_end_date"].iloc[0] if "data_end_date" in df.columns else df[x_col].max()
    print(f"Data covers: {data_start_date.date()} to {data_end_date.date()}")

    plt.figure(figsize=(10, 5))
    plt.plot(df[x_col], df["median_sale_price"], marker="o")
    plt.title(
        f"Median Sale Price \n"
        f"File: {parquet_path.name}\n"
        f"Data: {data_start_date.date()} to {data_end_date.date()}"
    )
    plt.xlabel(x_col.replace('_', ' ').title())
    plt.ylabel("Median sale price")
    plt.grid(True)
    plt.tight_layout()
    plt.show()

if __name__ == "__main__":
    main()
