from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Iterable

import pandas as pd
import yfinance as yf
from sqlalchemy import create_engine, text


@dataclass
class MarketDataConfig:
    db_uri: str
    symbols: list[str]


def fetch_daily_prices(symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
    """
    Fetch daily OHLCV data for one symbol from Yahoo Finance.

    Parameters
    ----------
    symbol : str
        Ticker symbol, e.g. 'AAPL'
    start_date : str
        Inclusive lower bound in YYYY-MM-DD format
    end_date : str
        Exclusive upper bound in YYYY-MM-DD format

    Returns
    -------
    pd.DataFrame
        Normalized dataframe with columns matching raw.daily_prices
    """
    df = yf.download(
        symbol,
        start=start_date,
        end=end_date,
        progress=False,
        auto_adjust=False,
    )

    if df.empty:
        return pd.DataFrame(
            columns=[
                "symbol",
                "trade_date",
                "open",
                "high",
                "low",
                "close",
                "volume",
            ]
        )

    # Flatten MultiIndex columns if present
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)

    df = df.reset_index()

    df = df.rename(
        columns={
            "Date": "trade_date",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Volume": "volume",
        }
    )

    df["symbol"] = symbol

    required_columns = [
        "symbol",
        "trade_date",
        "open",
        "high",
        "low",
        "close",
        "volume",
    ]

    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        raise ValueError(f"Missing expected columns for {symbol}: {missing}. Got: {list(df.columns)}")

    df = df[required_columns].copy()
    df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date

    return df


def fetch_prices_for_symbols(
    symbols: Iterable[str], start_date: str, end_date: str
) -> pd.DataFrame:
    """
    Fetch and combine daily prices for multiple symbols.
    """
    frames = []

    for symbol in symbols:
        symbol_df = fetch_daily_prices(symbol, start_date, end_date)
        if not symbol_df.empty:
            frames.append(symbol_df)

    if not frames:
        return pd.DataFrame(
            columns=[
                "symbol",
                "trade_date",
                "open",
                "high",
                "low",
                "close",
                "volume",
            ]
        )

    combined = pd.concat(frames, ignore_index=True)
    return combined


def upsert_daily_prices(df: pd.DataFrame, db_uri: str) -> int:
    """
    Insert rows into raw.daily_prices using ON CONFLICT for idempotency.

    Returns
    -------
    int
        Number of dataframe rows processed
    """
    if df.empty:
        return 0

    df = df.copy()
    df["ingestion_time"] = datetime.utcnow()

    engine = create_engine(db_uri)

    insert_sql = text(
        """
        INSERT INTO raw.daily_prices (
            symbol,
            trade_date,
            open,
            high,
            low,
            close,
            volume,
            ingestion_time
        )
        VALUES (
            :symbol,
            :trade_date,
            :open,
            :high,
            :low,
            :close,
            :volume,
            :ingestion_time
        )
        ON CONFLICT (symbol, trade_date)
        DO UPDATE SET
            open = EXCLUDED.open,
            high = EXCLUDED.high,
            low = EXCLUDED.low,
            close = EXCLUDED.close,
            volume = EXCLUDED.volume,
            ingestion_time = EXCLUDED.ingestion_time
        """
    )

    records = df.to_dict(orient="records")

    with engine.begin() as conn:
        conn.execute(insert_sql, records)

    return len(records)


def run_ingestion(
    db_uri: str, symbols: list[str], start_date: str, end_date: str
) -> int:
    """
    End-to-end helper for fetching and loading prices.
    """
    prices = fetch_prices_for_symbols(symbols, start_date, end_date)

    print(f"DEBUG start_date={start_date}, end_date={end_date}")
    print(f"DEBUG fetched row count={len(prices)}")
    if not prices.empty:
        print("DEBUG fetched columns:", list(prices.columns))
        print("DEBUG fetched sample:")
        print(prices.head(10).to_string(index=False))

    row_count = upsert_daily_prices(prices, db_uri)
    return row_count