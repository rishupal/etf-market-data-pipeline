"""
visualize.py
Data visualization utilities for ETF Market Data Pipeline.
Supports Python-based analytics and Tableau dashboard integration.
"""

import pandas as pd
import matplotlib.pyplot as plt


def plot_etf_price_trend(df: pd.DataFrame, ticker: str):
    """
    Plot closing price trend for a given ETF ticker.
    """
    etf_df = df[df["Ticker"] == ticker]
    plt.figure(figsize=(10, 5))
    plt.plot(etf_df["Date"], etf_df["Close"], marker="o")
    plt.title(f"{ticker} Closing Price Trend")
    plt.xlabel("Date")
    plt.ylabel("Close Price")
    plt.grid(True)
    plt.tight_layout()
    plt.show()


def export_for_tableau(df: pd.DataFrame, out_path: str = "data/tableau_etf_prices.csv"):
    """
    Export processed ETF data to CSV for Tableau dashboard integration.
    """
    df.to_csv(out_path, index=False)
    print(f"Exported data for Tableau: {out_path}")


if __name__ == "__main__":
    df = pd.read_csv("data/raw/etf_prices.csv", parse_dates=["Date"])
    plot_etf_price_trend(df, ticker="SPY")
    export_for_tableau(df)
