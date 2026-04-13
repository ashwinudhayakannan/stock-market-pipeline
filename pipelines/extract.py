import requests
import pandas as pd
import json
import os
import time
import logging

logger = logging.getLogger(__name__)

def load_config() -> dict:
    config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'tickers.json')
    with open(config_path) as f:
        return json.load(f)

def fetch_ticker(ticker: str, api_key: str, base_url: str) -> pd.DataFrame:
    """Fetch daily OHLCV data for one ticker from Alpha Vantage."""
    params = {
        "function": "TIME_SERIES_DAILY",
        "symbol": ticker,
        "outputsize": "compact",   # last 100 trading days
        "apikey": api_key
    }

    response = requests.get(base_url, params=params, timeout=30)
    response.raise_for_status()
    data = response.json()

    if "Time Series (Daily)" not in data:
        raise ValueError(f"Unexpected API response for {ticker}: {data}")

    records = []
    for date_str, values in data["Time Series (Daily)"].items():
        records.append({
            "ticker":       ticker,
            "trade_date":   date_str,
            "open_price":   float(values["1. open"]),
            "high_price":   float(values["2. high"]),
            "low_price":    float(values["3. low"]),
            "close_price":  float(values["4. close"]),
            "volume":       int(values["5. volume"])
        })

    df = pd.DataFrame(records)
    df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
    logger.info(f"Fetched {len(df)} rows for {ticker}")
    return df

def extract_all_tickers() -> pd.DataFrame:
    """Fetch data for all tickers and return a combined DataFrame."""
    config  = load_config()
    api_key = os.getenv("ALPHA_VANTAGE_API_KEY")

    if not api_key:
        raise EnvironmentError("ALPHA_VANTAGE_API_KEY not set in environment")

    all_dfs = []

    for ticker in config["tickers"]:
        try:
            df = fetch_ticker(ticker, api_key, config["api_base_url"])
            all_dfs.append(df)
            time.sleep(12)  # Alpha Vantage free tier: 5 calls/min
        except Exception as e:
            logger.error(f"Failed to fetch {ticker}: {e}")
            raise

    combined = pd.concat(all_dfs, ignore_index=True)
    logger.info(f"Total rows extracted: {len(combined)}")
    return combined