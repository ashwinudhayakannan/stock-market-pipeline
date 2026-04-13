import pandas as pd
from sqlalchemy import create_engine, text
import os
import logging

logger = logging.getLogger(__name__)

def get_engine():
    user     = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")
    host     = os.getenv("POSTGRES_HOST")
    port     = os.getenv("POSTGRES_PORT", "5432")
    db       = "stock_db"   # your pipeline DB, not airflow_meta
    return create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}")

def load_raw(df: pd.DataFrame) -> None:
    """Upsert raw stock prices into raw_stock_prices."""
    engine = get_engine()

    with engine.begin() as conn:
        for _, row in df.iterrows():
            conn.execute(text("""
                INSERT INTO raw_stock_prices
                    (ticker, trade_date, open_price, high_price,
                     low_price, close_price, volume)
                VALUES
                    (:ticker, :trade_date, :open_price, :high_price,
                     :low_price, :close_price, :volume)
                ON CONFLICT (ticker, trade_date)
                DO UPDATE SET
                    open_price  = EXCLUDED.open_price,
                    high_price  = EXCLUDED.high_price,
                    low_price   = EXCLUDED.low_price,
                    close_price = EXCLUDED.close_price,
                    volume      = EXCLUDED.volume,
                    ingested_at = NOW()
            """), row.to_dict())

    logger.info(f"Upserted {len(df)} rows into raw_stock_prices")