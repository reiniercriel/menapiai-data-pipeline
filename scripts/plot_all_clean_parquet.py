"""
Plot all Parquet files under data/clean and save PNGs under data/plots.

- Housing files (housing_trends_*): plot median sale price, homes sold,
  inventory, and median days on market when present.
- Employment files (employment_trends_*): plot the single measure over time.
"""
from __future__ import annotations

import re
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt

CLEAN_DIR = Path("data/clean")
PLOTS_DIR = Path("data/plots")
PLOTS_DIR.mkdir(parents=True, exist_ok=True)


def _infer_datetime(df: pd.DataFrame, date_cols: list[str]) -> pd.Series:
    for c in date_cols:
        if c in df.columns:
            try:
                return pd.to_datetime(df[c])
            except Exception:
                continue
    # Try YEAR + MONTH combination
    if {"year", "month"}.issubset({c.lower() for c in df.columns}):
        # handle case-sensitive
        ycol = next(c for c in df.columns if c.lower() == "year")
        mcol = next(c for c in df.columns if c.lower() == "month")
        return pd.to_datetime(
            df[ycol].astype(int).astype(str)
            + "-"
            + df[mcol].astype(int).astype(str).str.zfill(2)
            + "-01"
        )
    raise ValueError("Could not infer date column for plotting")


def plot_housing(df: pd.DataFrame, title: str, out_path: Path) -> None:
    metrics = [
        ("median_sale_price", "Median Sale Price"),
        ("homes_sold", "Homes Sold"),
        ("inventory", "Inventory"),
        ("median_days_on_market", "Median Days on Market"),
    ]
    period_series = _infer_datetime(
        df, ["period_begin", "period", "date", "period_end"]
    )

    n = 0
    for col, _ in metrics:
        if col in df.columns:
            n += 1
    if n == 0:
        return

    rows, cols = (2, 2)
    fig, axes = plt.subplots(rows, cols, figsize=(12, 8), constrained_layout=True)
    axes = axes.flatten()

    idx = 0
    for col, label in metrics:
        if col not in df.columns:
            continue
        ax = axes[idx]
        ax.plot(period_series, df[col])
        ax.set_title(label)
        ax.grid(True, alpha=0.3)
        idx += 1
        if idx >= rows * cols:
            break

    fig.suptitle(title)
    fig.autofmt_xdate()
    fig.savefig(out_path)
    plt.close(fig)


def plot_employment(df: pd.DataFrame, title: str, out_path: Path) -> None:
    # Identify measure column by known names
    measure_cols = [
        "labor_force",
        "employed",
        "unemployed",
        "unemployment_rate",
    ]
    present = [c for c in measure_cols if c in df.columns]
    if not present:
        return
    col = present[0]

    period_series = _infer_datetime(df, ["period", "date"])  # PERIOD = YYYY-MM

    fig, ax = plt.subplots(figsize=(12, 4))
    ax.plot(period_series, df[col])
    ax.set_title(f"{title} — {col}")
    ax.grid(True, alpha=0.3)
    fig.autofmt_xdate()
    fig.savefig(out_path)
    plt.close(fig)


def _slug(s: str) -> str:
    return re.sub(r"[^a-z0-9_]+", "_", s.lower()).strip("_")


def main() -> None:
    if not CLEAN_DIR.exists():
        print(f"No clean directory found at {CLEAN_DIR}")
        return

    # Prefer partitioned datasets if present
    housing_ds = CLEAN_DIR / "housing_trends"
    if housing_ds.exists():
        try:
            dfh = pd.read_parquet(housing_ds)
            if "property_type" in dfh.columns:
                for ptype, df_ht in dfh.groupby("property_type"):
                    title = f"Housing Trends — {ptype}"
                    out = PLOTS_DIR / f"housing_trends_{_slug(str(ptype))}.png"
                    try:
                        plot_housing(df_ht, title, out)
                        print(f"Saved plot: {out}")
                    except Exception as e:
                        print(f"Failed housing plot for {ptype}: {e}")
        except Exception as e:
            print(f"Failed reading housing dataset: {e}")

    employment_ds = CLEAN_DIR / "employment_trends"
    if employment_ds.exists():
        try:
            dfe = pd.read_parquet(employment_ds)
            # group by region if present, else single plot
            if "region_name" in dfe.columns:
                groups = dfe.groupby("region_name")
            elif "region_partition" in dfe.columns:
                groups = dfe.groupby("region_partition")
            else:
                groups = [("region", dfe)]

            for region, df_rg in groups:
                # Create a 2x2 of all measures for this region
                period_series = _infer_datetime(df_rg, ["period"])  # YYYY-MM
                measures = [
                    ("labor_force", "Labor Force"),
                    ("employed", "Employed"),
                    ("unemployed", "Unemployed"),
                    ("unemployment_rate", "Unemployment Rate"),
                ]
                fig, axes = plt.subplots(2, 2, figsize=(12, 8), constrained_layout=True)
                axes = axes.flatten()
                for i, (col, label) in enumerate(measures):
                    ax = axes[i]
                    if col in df_rg.columns:
                        ax.plot(period_series, df_rg[col])
                    ax.set_title(label)
                    ax.grid(True, alpha=0.3)
                fig.suptitle(f"Employment Trends — {region}")
                fig.autofmt_xdate()
                out = PLOTS_DIR / f"employment_trends_{_slug(str(region))}.png"
                fig.savefig(out)
                plt.close(fig)
                print(f"Saved plot: {out}")
        except Exception as e:
            print(f"Failed reading employment dataset: {e}")

    # Backward-compat: also plot any flat Parquet files at the root
    parquet_files = sorted(CLEAN_DIR.glob("*.parquet"))
    for p in parquet_files:
        try:
            df = pd.read_parquet(p)
        except Exception as e:
            print(f"Skipping {p.name}: failed to read Parquet ({e})")
            continue

        base = p.stem
        out_path = PLOTS_DIR / f"{base}.png"

        if base.startswith("housing_trends_"):
            title = base.replace("_", " ")
            try:
                plot_housing(df, title, out_path)
                print(f"Saved plot: {out_path}")
            except Exception as e:
                print(f"Failed housing plot for {p.name}: {e}")
        elif base.startswith("employment_trends_"):
            # If flat file has only one measure, plot it
            try:
                plot_employment(df, "Employment Trends", out_path)
                print(f"Saved plot: {out_path}")
            except Exception as e:
                print(f"Failed employment plot for {p.name}: {e}")


if __name__ == "__main__":
    main()
