from sqlalchemy import text
import os
import logging
from pipelines.load import get_engine

logger = logging.getLogger(__name__)

def run_transform() -> None:
    """Execute the SQL transformation to populate stock_daily_summary."""
    sql_path = os.path.join(os.path.dirname(__file__), '..', 'sql', 'transform_summary.sql')

    with open(sql_path) as f:
        sql = f.read()

    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text(sql))

    logger.info("Transform complete — stock_daily_summary updated")