import pandas as pd

def validate_data(df: pd.DataFrame) -> None:
    if df["Date"].isnull().any():
        raise ValueError("Missing dates detected")

    if df.duplicated(subset=["Date"]).any():
        raise ValueError("Duplicate dates detected")

    if (df["Close"] <= 0).any():
        raise ValueError("Invalid close prices detected")

    print("Data validation passed")

if __name__ == "__main__":
    df = pd.read_csv("data/raw/etf_prices.csv", parse_dates=["Date"])
    validate_data(df)
