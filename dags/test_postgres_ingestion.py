from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


def insert_test_data() -> None:
    hook = PostgresHook(postgres_conn_id="postgres_default")

    rows = [
        ("AAPL", 172.35, "2026-03-25 09:30:00"),
        ("MSFT", 421.12, "2026-03-25 09:30:00"),
        ("GOOGL", 158.77, "2026-03-25 09:30:00"),
    ]

    insert_sql = """
        INSERT INTO raw.test_market_data (symbol, price, event_time)
        VALUES (%s, %s, %s)
    """

    conn = hook.get_conn()
    cursor = conn.cursor()

    for row in rows:
        cursor.execute(insert_sql, row)

    conn.commit()
    cursor.close()
    conn.close()


with DAG(
    dag_id="test_postgres_ingestion",
    start_date=datetime(2026, 3, 25),
    schedule=None,
    catchup=False,
    tags=["test", "postgres"],
) as dag:
    insert_task = PythonOperator(
        task_id="insert_test_data",
        python_callable=insert_test_data,
    )