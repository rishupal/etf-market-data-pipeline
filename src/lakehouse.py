"""
lakehouse.py
Simulates a metadata-driven, incrementally loading data lakehouse (AWS S3-like).
Aligns with cloud migration, incremental processing, and data governance best practices.
"""

import pandas as pd
from pathlib import Path
from metadata import validate_schema

LAKEHOUSE_ROOT = Path("data/lakehouse/")


def save_partition(df: pd.DataFrame, partition_col: str = "Date"):
    """
    Save DataFrame partitions by year/month to lakehouse directory.
    Only saves partitions with valid schema.
    """
    if not validate_schema(df):
        raise ValueError("Invalid schema for lakehouse storage.")
    df[partition_col] = pd.to_datetime(df[partition_col])
    for (year, month), part in df.groupby([df[partition_col].dt.year, df[partition_col].dt.month]):
        part_dir = LAKEHOUSE_ROOT / f"year={year}" / f"month={month:02d}"
        part_dir.mkdir(parents=True, exist_ok=True)
        part_path = part_dir / "etf_prices.parquet"
        part.to_parquet(part_path, index=False)


def load_lakehouse():
    """
    Load all partitions from lakehouse into a single DataFrame.
    """
    dfs = []
    for year_dir in LAKEHOUSE_ROOT.glob("year=*"):
        for month_dir in year_dir.glob("month=*"):
            part_path = month_dir / "etf_prices.parquet"
            if part_path.exists():
                dfs.append(pd.read_parquet(part_path))
    if dfs:
        return pd.concat(dfs, ignore_index=True)
    else:
        return pd.DataFrame()


def incremental_update(new_data: pd.DataFrame):
    """
    Incrementally update lakehouse with new data (avoids duplicates).
    """
    lakehouse_df = load_lakehouse()
    if not lakehouse_df.empty:
        combined = pd.concat([lakehouse_df, new_data], ignore_index=True)
        combined = combined.drop_duplicates(subset=["Date", "Ticker"])
    else:
        combined = new_data.drop_duplicates(subset=["Date", "Ticker"])
    save_partition(combined)


if __name__ == "__main__":
    # Example usage
    df = pd.read_csv("data/raw/etf_prices.csv", parse_dates=["Date"])
    incremental_update(df)
    print("Lakehouse updated.")
