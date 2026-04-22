from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from src.ingestion.market_data import run_ingestion


DB_URI = "postgresql+psycopg2://airflow:airflow@postgres:5432/airflow"
SYMBOLS = ["AAPL", "MSFT", "GOOGL"]


def ingest_market_data(**context) -> None:
    """
    Ingest daily OHLCV data for the DAG's data interval.
    """
    data_interval_start = context["data_interval_start"]
    data_interval_end = context["data_interval_end"]

    start_date = data_interval_start.strftime("%Y-%m-%d")
    end_date = data_interval_end.strftime("%Y-%m-%d")

    print(f"DEBUG DAG start_date={start_date}, end_date={end_date}")

    rows_processed = run_ingestion(
        db_uri=DB_URI,
        symbols=SYMBOLS,
        start_date=start_date,
        end_date=end_date,
    )

    print(
        f"Ingestion complete for interval {start_date} to {end_date}. "
        f"Processed rows: {rows_processed}"
    )


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="ingest_daily_prices",
    default_args=default_args,
    description="Ingest daily OHLCV price data into raw.daily_prices",
    start_date=datetime(2026, 4, 1),
    schedule="@daily",
    catchup=False,
    tags=["equity", "ingestion"],
) as dag:
    ingest_task = PythonOperator(
        task_id="ingest_market_data",
        python_callable=ingest_market_data,
    )

    ingest_task