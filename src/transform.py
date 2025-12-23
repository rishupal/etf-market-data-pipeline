import pandas as pd
from pathlib import Path

PROCESSED_PATH = Path("data/processed/etf_cleaned.csv")

def transform(df: pd.DataFrame) -> pd.DataFrame:
    df["DailyReturn"] = df["Close"].pct_change()
    return df

def incremental_load(df: pd.DataFrame) -> pd.DataFrame:
    if PROCESSED_PATH.exists():
        existing = pd.read_csv(PROCESSED_PATH, parse_dates=["Date"])
        df = df[df["Date"] > existing["Date"].max()]
        df = pd.concat([existing, df])
    return df

if __name__ == "__main__":
    df = pd.read_csv("data/raw/etf_prices.csv", parse_dates=["Date"])
    df = transform(df)
    df = incremental_load(df)
    PROCESSED_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(PROCESSED_PATH, index=False)
