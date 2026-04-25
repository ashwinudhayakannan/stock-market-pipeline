import os
import logging
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text

logger = logging.getLogger(__name__)

DB_NAME = "stock_db"
DEFAULT_ADMIN_DB = "postgres"

SQL_DIR = Path(__file__).resolve().parent.parent / "sql"
SCHEMA_SQL_PATH = SQL_DIR / "create_tables.sql"


def get_engine(db_name: str = DB_NAME):
    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")
    host = os.getenv("POSTGRES_HOST")
    port = os.getenv("POSTGRES_PORT", "5432")

    if not all([user, password, host]):
        raise EnvironmentError("POSTGRES_USER, POSTGRES_PASSWORD, and POSTGRES_HOST must be set")

    return create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db_name}")


def get_admin_engine():
    admin_db = os.getenv("POSTGRES_ADMIN_DB", DEFAULT_ADMIN_DB)
    return get_engine(db_name=admin_db)


def ensure_database_exists() -> None:
    engine = get_admin_engine()
    with engine.connect() as conn:
        exists = conn.execute(
            text("SELECT 1 FROM pg_database WHERE datname = :db"),
            {"db": DB_NAME}
        ).scalar()
        if not exists:
            conn.execute(text(f"CREATE DATABASE \"{DB_NAME}\""))
            logger.info(f"Created missing database: {DB_NAME}")


def ensure_tables_exist() -> None:
    engine = get_engine()
    with engine.begin() as conn:
        with SCHEMA_SQL_PATH.open("r", encoding="utf-8") as f:
            sql = f.read()
        conn.exec_driver_sql(sql)
    logger.info("Ensured pipeline table schema exists")


def ensure_database_and_tables() -> None:
    ensure_database_exists()
    ensure_tables_exist()


def load_raw(df: pd.DataFrame) -> None:
    """Upsert raw stock prices into raw_stock_prices."""
    ensure_database_and_tables()
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