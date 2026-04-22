from src.ingestion.market_data import run_ingestion

DB_URI = "postgresql+psycopg2://airflow:airflow@localhost:5432/airflow"
SYMBOLS = ["AAPL", "MSFT", "GOOGL"]

rows = run_ingestion(
    db_uri=DB_URI,
    symbols=SYMBOLS,
    start_date="2026-04-01",
    end_date="2026-04-10",
)

print(f"Processed rows: {rows}")