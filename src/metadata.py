"""
metadata.py
Centralized schema and metadata management for ETF Market Data Pipeline.
Aligns with metadata-driven architecture and data governance best practices.
"""

ETF_SCHEMA = {
    "Date": "datetime64[ns]",
    "Ticker": "str",
    "Open": "float",
    "High": "float",
    "Low": "float",
    "Close": "float",
    "Volume": "int",
}


def get_schema():
    """Return the expected ETF data schema."""
    return ETF_SCHEMA


def validate_schema(df):
    """
    Validate DataFrame columns and dtypes against expected schema.
    Returns True if valid, else False.
    """
    expected_cols = set(ETF_SCHEMA.keys())
    df_cols = set(df.columns)
    if expected_cols != df_cols:
        return False
    for col, dtype in ETF_SCHEMA.items():
        if dtype.startswith("datetime"):
            if not str(df[col].dtype).startswith("datetime"):
                return False
        elif dtype == "str":
            if df[col].dtype != "object":
                return False
        elif dtype == "float":
            if not ("float" in str(df[col].dtype)):
                return False
        elif dtype == "int":
            if not ("int" in str(df[col].dtype)):
                return False
    return True


if __name__ == "__main__":
    import pandas as pd
    # Example usage
    df = pd.read_csv("data/raw/etf_prices.csv", parse_dates=["Date"])
    print("Schema valid:", validate_schema(df))
