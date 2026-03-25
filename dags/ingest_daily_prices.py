from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from src.ingestion.market_data import fetch_and_store_daily_prices


def run_ingestion(**context) -> None:
    symbols = ["AAPL", "MSFT", "GOOGL"]

    data_interval_start = context["data_interval_start"]
    data_interval_end = context["data_interval_end"]

    start_date = data_interval_start.strftime("%Y-%m-%d")
    end_date = data_interval_end.strftime("%Y-%m-%d")

    fetch_and_store_daily_prices(
        symbols=symbols,
        start_date=start_date,
        end_date=end_date,
    )


with DAG(
    dag_id="ingest_daily_prices",
    start_date=datetime(2026, 3, 25),
    schedule="@daily",
    catchup=False,
    tags=["market-data", "ingestion"],
) as dag:
    ingest_task = PythonOperator(
        task_id="fetch_and_store_daily_prices",
        python_callable=run_ingestion,
    )