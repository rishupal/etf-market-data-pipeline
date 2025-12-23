"""
dimensional_model.py
Example dimensional modeling for ETF Market Data Pipeline.
Includes star schema and Data Vault concepts for analytics-ready data.
"""

import pandas as pd

def create_star_schema(df: pd.DataFrame):
    """
    Transform raw ETF data into star schema: fact table and dimension tables.
    Returns dict of DataFrames.
    """
    # Dimension: Date
    date_dim = df[["Date"]].drop_duplicates().copy()
    date_dim["date_id"] = date_dim["Date"].astype(str)
    # Dimension: ETF
    etf_dim = df[["Ticker"]].drop_duplicates().copy()
    etf_dim["etf_id"] = etf_dim["Ticker"]
    # Fact table
    fact = df.merge(date_dim, on="Date").merge(etf_dim, on="Ticker")
    fact_table = fact[[
        "date_id", "etf_id", "Open", "High", "Low", "Close", "Volume"
    ]]
    return {
        "date_dim": date_dim,
        "etf_dim": etf_dim,
        "fact_table": fact_table
    }


def create_data_vault(df: pd.DataFrame):
    """
    Transform raw ETF data into Data Vault model: hub, link, satellite.
    Returns dict of DataFrames.
    """
    # Hub: ETF
    hub_etf = df[["Ticker"]].drop_duplicates().copy()
    hub_etf["hub_etf_id"] = hub_etf["Ticker"]
    # Hub: Date
    hub_date = df[["Date"]].drop_duplicates().copy()
    hub_date["hub_date_id"] = hub_date["Date"].astype(str)
    # Link: ETF-Date
    link_etf_date = df[["Ticker", "Date"]].drop_duplicates().copy()
    link_etf_date = link_etf_date.merge(hub_etf, on="Ticker").merge(hub_date, on="Date")
    link_etf_date["link_id"] = link_etf_date["hub_etf_id"] + "_" + link_etf_date["hub_date_id"]
    # Satellite: Prices
    sat_prices = df.copy()
    sat_prices["sat_id"] = sat_prices["Ticker"] + "_" + sat_prices["Date"].astype(str)
    return {
        "hub_etf": hub_etf,
        "hub_date": hub_date,
        "link_etf_date": link_etf_date,
        "sat_prices": sat_prices
    }


if __name__ == "__main__":
    df = pd.read_csv("data/raw/etf_prices.csv", parse_dates=["Date"])
    star = create_star_schema(df)
    vault = create_data_vault(df)
    print("Star schema tables:", list(star.keys()))
    print("Data Vault tables:", list(vault.keys()))
