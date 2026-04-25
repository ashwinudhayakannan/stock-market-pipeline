from sqlalchemy import text
import os
import logging
from pathlib import Path
from datetime import datetime
import pandas as pd
from pipelines.load import get_engine, ensure_database_and_tables

logger = logging.getLogger(__name__)

OUTPUT_DIR = Path(__file__).resolve().parent.parent / "output"


def ensure_output_dir() -> None:
    """Create output directory if it doesn't exist."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    logger.info(f"Output directory ensured: {OUTPUT_DIR}")


def run_transform() -> None:
    """Execute the SQL transformation to populate stock_daily_summary."""
    ensure_database_and_tables()
    sql_path = os.path.join(os.path.dirname(__file__), '..', 'sql', 'transform_summary.sql')

    with open(sql_path) as f:
        sql = f.read()

    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text(sql))

    logger.info("Transform complete — stock_daily_summary updated")


def export_summary_to_csv() -> str:
    """Export stock_daily_summary table to CSV with timestamp."""
    ensure_database_and_tables()
    ensure_output_dir()
    
    engine = get_engine()
    
    # Read the transformed data using connection
    with engine.connect() as conn:
        df = pd.read_sql("SELECT * FROM stock_daily_summary ORDER BY ticker, trade_date", conn)
    
    # Generate filename with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_file = OUTPUT_DIR / f"stock_daily_summary_{timestamp}.csv"
    
    # Save to CSV
    df.to_csv(csv_file, index=False)
    logger.info(f"Exported {len(df)} rows to {csv_file}")
    
    return str(csv_file)
