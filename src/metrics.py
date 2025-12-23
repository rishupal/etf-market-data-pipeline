import pandas as pd
import numpy as np

def calculate_metrics(df: pd.DataFrame) -> dict:
    cagr = (df["Close"].iloc[-1] / df["Close"].iloc[0]) ** (252 / len(df)) - 1
    volatility = df["DailyReturn"].std() * np.sqrt(252)

    return {
        "CAGR": round(cagr, 4),
        "Volatility": round(volatility, 4)
    }

if __name__ == "__main__":
    df = pd.read_csv("data/processed/etf_cleaned.csv")
    metrics = calculate_metrics(df)
    print(metrics)
