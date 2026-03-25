from __future__ import annotations

from typing import Iterable

import yfinance as yf
from airflow.providers.postgres.hooks.postgres import PostgresHook


def fetch_and_store_daily_prices(
    symbols: Iterable[str],
    start_date: str,
    end_date: str,
) -> None:
    hook = PostgresHook(postgres_conn_id="postgres_default")

    conn = hook.get_conn()
    cursor = conn.cursor()

    insert_sql = """
        INSERT INTO raw.daily_prices (
            symbol, trade_date, open, high, low, close, volume
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (symbol, trade_date) DO UPDATE SET
            open = EXCLUDED.open,
            high = EXCLUDED.high,
            low = EXCLUDED.low,
            close = EXCLUDED.close,
            volume = EXCLUDED.volume,
            ingestion_time = CURRENT_TIMESTAMP
    """

    for symbol in symbols:
        df = yf.download(
            symbol,
            start=start_date,
            end=end_date,
            interval="1d",
            auto_adjust=False,
            progress=False,
        )

        if df.empty:
            continue

        for trade_date, row in df.iterrows():
            cursor.execute(
                insert_sql,
                (
                    symbol,
                    trade_date.date(),
                    float(row["Open"]) if row["Open"] is not None else None,
                    float(row["High"]) if row["High"] is not None else None,
                    float(row["Low"]) if row["Low"] is not None else None,
                    float(row["Close"]) if row["Close"] is not None else None,
                    int(row["Volume"]) if row["Volume"] is not None else None,
                ),
            )

    conn.commit()
    cursor.close()
    conn.close()