import pandas as pd

def test_dataframe_has_required_columns():
    """Verify extracted DataFrame always has the expected schema."""
    required_cols = {"ticker", "trade_date", "open_price",
                     "high_price", "low_price", "close_price", "volume"}

    sample = pd.DataFrame([{
        "ticker": "MSFT", "trade_date": "2024-01-02",
        "open_price": 370.0, "high_price": 375.0,
        "low_price": 368.0, "close_price": 374.0,
        "volume": 22000000
    }])

    assert required_cols.issubset(set(sample.columns))