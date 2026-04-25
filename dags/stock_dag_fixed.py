from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import sys
import os

sys.path.insert(0, '/opt/airflow')

from pipelines.extract   import extract_all_tickers
from pipelines.load      import load_raw
from pipelines.transform import run_transform, export_summary_to_csv

default_args = {
    "owner":            "ashwin",
    "retries":          2,
    "retry_delay":      timedelta(seconds=20),
    "email_on_failure": True,
    "email":            ["ashwinat032@email.com"],
}

def extract_and_push(**context):
    df = extract_all_tickers()
    context["ti"].xcom_push(key="raw_df", value=df.to_json())

def load_from_xcom(**context):
    import pandas as pd
    import io
    raw_json = context["ti"].xcom_pull(key="raw_df", task_ids="extract")
    df = pd.read_json(io.StringIO(raw_json))
    df["trade_date"] = pd.to_datetime(df["trade_date"], unit="ms").dt.date
    load_raw(df)

def quality_check(**context):
    import pandas as pd
    raw_json = context["ti"].xcom_pull(key="raw_df", task_ids="extract")
    import io
    df = pd.read_json(io.StringIO(raw_json))
    assert len(df) > 0, "Quality check failed: DataFrame is empty"
    assert df["close_price"].notnull().all(), "Quality check failed: nulls in close_price"
    assert (df["close_price"] > 0).all(), "Quality check failed: non-positive close prices"
    import logging
    logging.getLogger(__name__).info(f"Quality check passed: {len(df)} rows, no nulls")

with DAG(
    dag_id="stock_pipeline",
    description="Daily stock price ELT pipeline",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval="0 * * * *",
    catchup=False,
    tags=["finance", "stocks", "elt"]
) as dag:

    t_start = EmptyOperator(task_id="start")

    t_extract = PythonOperator(
        task_id="extract",
        python_callable=extract_and_push,
        provide_context=True
    )

    t_quality = PythonOperator(
        task_id="quality_check",
        python_callable=quality_check,
        provide_context=True
    )

    t_load = PythonOperator(
        task_id="load_raw",
        python_callable=load_from_xcom,
        provide_context=True
    )

    t_transform = PythonOperator(
        task_id="transform",
        python_callable=run_transform,
        provide_context=True
    )

    t_export_csv = PythonOperator(
        task_id="export_to_csv",
        python_callable=export_summary_to_csv,
        provide_context=True
    )

    t_end = EmptyOperator(task_id="end")

    t_start >> t_extract >> t_quality >> t_load >> t_transform >> t_export_csv >> t_end
