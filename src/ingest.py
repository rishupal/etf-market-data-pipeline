import pandas as pd
from pathlib import Path

RAW_DATA_PATH = Path("data/raw/etf_prices.csv")

def ingest_data(csv_path: str) -> pd.DataFrame:
    df = pd.read_csv(csv_path, parse_dates=["Date"])
    df = df.sort_values("Date")
    return df

if __name__ == "__main__":
    df = ingest_data("data/raw/source_etf_data.csv")
    RAW_DATA_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(RAW_DATA_PATH, index=False)
    print(f"Ingested {len(df)} records")
