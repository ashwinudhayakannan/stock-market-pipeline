import pandas as pd
from unittest.mock import patch, MagicMock
from pipelines.extract import fetch_ticker

MOCK_RESPONSE = {
    "Time Series (Daily)": {
        "2024-01-02": {
            "1. open": "185.00",
            "2. high": "187.00",
            "3. low": "184.00",
            "4. close": "186.00",
            "5. volume": "55000000"
        }
    }
}

@patch("pipelines.extract.requests.get")
def test_fetch_ticker_returns_dataframe(mock_get):
    mock_get.return_value = MagicMock(
        status_code=200,
        json=lambda: MOCK_RESPONSE
    )
    mock_get.return_value.raise_for_status = lambda: None

    df = fetch_ticker("AAPL", "fake_key", "https://fake.url")

    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1
    assert df.iloc[0]["ticker"] == "AAPL"
    assert df.iloc[0]["close_price"] == 186.0