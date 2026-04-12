"""
YFinance data producer for MarketLens.

Phase 1: fetch_and_write_to_snowflake — writes directly to RAW_STOCK_PRICES.
Phase 2: fetch_and_publish_to_kafka   — publishes to Kafka topic (uncomment
         kafka-python in requirements.txt first).

Inspired by daily_stock_analysis/data_provider/yfinance_fetcher.py:
- Retry with tenacity (exponential backoff)
- Normalizes MultiIndex columns from newer yfinance versions
- MERGE INTO Snowflake for idempotent writes (no duplicates on replay)
"""

from __future__ import annotations

import json
import logging
import os
import sys
import uuid
from datetime import date, timedelta
from typing import List, Optional

from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
)

# Make root importable when called from DAG context
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from ingestion.base_producer import BaseProducer, UnifiedQuote, DataFetchError
import config as cfg

logger = logging.getLogger(__name__)


class YFinanceProducer(BaseProducer):
    """
    Fetches daily OHLCV from Yahoo Finance for US equities.

    Priority 1 (primary source).  Falls back gracefully — if yfinance
    returns an empty DataFrame the error propagates and the circuit breaker
    records the failure.
    """

    name     = 'yfinance'
    priority = 1

    def __init__(self):
        super().__init__(
            failure_threshold=cfg.CIRCUIT_BREAKER_FAILURES,
            cooldown_seconds=cfg.CIRCUIT_BREAKER_COOLDOWN,
        )

    # ------------------------------------------------------------------
    # Core fetch (retried by tenacity)
    # ------------------------------------------------------------------

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        retry=retry_if_exception_type((ConnectionError, TimeoutError, OSError)),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True,
    )
    def _fetch_raw(self, tickers: List[str], start: date, end: date):
        """Download raw DataFrame from yfinance (retried on network errors)."""
        import yfinance as yf
        df = yf.download(
            tickers=tickers,
            start=start.isoformat(),
            end=end.isoformat(),
            progress=False,
            auto_adjust=True,
            multi_level_index=True,
        )
        if df.empty:
            raise DataFetchError(f'yfinance returned empty DataFrame for {tickers} on {start}')
        return df

    # ------------------------------------------------------------------
    # Normalize yfinance MultiIndex → list[UnifiedQuote]
    # ------------------------------------------------------------------

    def _normalize(self, df, tickers: List[str], target_date: date,
                   query_id: str) -> List[UnifiedQuote]:
        """
        yfinance returns a MultiIndex DataFrame: (Price, Ticker).
        We extract each ticker's row and build UnifiedQuote objects.
        """
        import pandas as pd

        quotes: List[UnifiedQuote] = []

        for ticker in tickers:
            try:
                # Flatten MultiIndex for this ticker
                if isinstance(df.columns, pd.MultiIndex):
                    ticker_df = df.xs(ticker, axis=1, level=1)
                else:
                    ticker_df = df.copy()

                # Find the row closest to target_date
                ticker_df.index = pd.to_datetime(ticker_df.index).date
                if target_date not in ticker_df.index:
                    # Use the last available row (handles weekends/holidays)
                    row = ticker_df.iloc[-1]
                else:
                    row = ticker_df.loc[target_date]

                quotes.append(UnifiedQuote(
                    ticker=ticker,
                    date=target_date,
                    open=float(row['Open'])   if 'Open'   in row and row['Open']   == row['Open'] else None,
                    high=float(row['High'])   if 'High'   in row and row['High']   == row['High'] else None,
                    low=float(row['Low'])     if 'Low'    in row and row['Low']    == row['Low']  else None,
                    close=float(row['Close']) if 'Close'  in row and row['Close']  == row['Close'] else None,
                    volume=int(row['Volume']) if 'Volume' in row and row['Volume'] == row['Volume'] else None,
                    source=self.name,
                    query_id=query_id,
                ))
            except Exception as exc:
                logger.warning('Could not normalize %s: %s', ticker, exc)

        return quotes

    # ------------------------------------------------------------------
    # BaseProducer interface
    # ------------------------------------------------------------------

    def fetch(self, tickers: List[str], target_date: date) -> List[UnifiedQuote]:
        query_id = str(uuid.uuid4())
        # Download one extra day so we always have data even if target_date
        # falls on a holiday (yfinance returns the prior trading day).
        end = target_date + timedelta(days=3)
        try:
            df = self._fetch_raw(tickers, target_date - timedelta(days=1), end)
        except DataFetchError:
            raise
        except Exception as exc:
            raise DataFetchError(f'yfinance download failed: {exc}') from exc

        return self._normalize(df, tickers, target_date, query_id)

    # ------------------------------------------------------------------
    # Phase 1: direct Snowflake write
    # ------------------------------------------------------------------

    def fetch_and_write_to_snowflake(
        self,
        tickers: List[str],
        target_date: date,
        conn,
    ) -> int:
        """
        Fetch quotes and MERGE into RAW_STOCK_PRICES.
        Returns number of rows written.
        """
        quotes = self.fetch_with_guard(tickers, target_date)
        if not quotes:
            logger.warning('No quotes returned for %s', target_date)
            return 0

        cursor = conn.cursor()
        try:
            merge_sql = """
                MERGE INTO SCORPION_DB.MARKETLENS.RAW_STOCK_PRICES AS tgt
                USING (
                    SELECT
                        %(ticker)s    AS TICKER,
                        %(date)s      AS DATE,
                        %(open)s      AS OPEN_PRICE,
                        %(high)s      AS HIGH_PRICE,
                        %(low)s       AS LOW_PRICE,
                        %(close)s     AS CLOSE_PRICE,
                        %(volume)s    AS VOLUME,
                        %(source)s    AS SOURCE,
                        %(query_id)s  AS QUERY_ID
                ) AS src ON (tgt.TICKER = src.TICKER
                         AND tgt.DATE   = src.DATE
                         AND tgt.SOURCE = src.SOURCE)
                WHEN MATCHED THEN UPDATE SET
                    OPEN_PRICE  = src.OPEN_PRICE,
                    HIGH_PRICE  = src.HIGH_PRICE,
                    LOW_PRICE   = src.LOW_PRICE,
                    CLOSE_PRICE = src.CLOSE_PRICE,
                    VOLUME      = src.VOLUME,
                    QUERY_ID    = src.QUERY_ID,
                    INGESTED_AT = CURRENT_TIMESTAMP()
                WHEN NOT MATCHED THEN INSERT
                    (TICKER, DATE, OPEN_PRICE, HIGH_PRICE, LOW_PRICE,
                     CLOSE_PRICE, VOLUME, SOURCE, QUERY_ID)
                VALUES
                    (src.TICKER, src.DATE, src.OPEN_PRICE, src.HIGH_PRICE,
                     src.LOW_PRICE, src.CLOSE_PRICE, src.VOLUME,
                     src.SOURCE, src.QUERY_ID)
            """
            for q in quotes:
                cursor.execute(merge_sql, q.to_dict())
            logger.info('Merged %d rows into RAW_STOCK_PRICES for %s', len(quotes), target_date)
            return len(quotes)
        finally:
            cursor.close()

    # ------------------------------------------------------------------
    # Phase 2: publish to Kafka (requires kafka-python)
    # ------------------------------------------------------------------

    def fetch_and_publish_to_kafka(
        self,
        tickers: List[str],
        target_date: date,
        kafka_producer,
        topic: Optional[str] = None,
    ) -> int:
        """
        Fetch quotes and publish each as a JSON message to Kafka.
        Key = 'TICKER:DATE' for deduplication.
        Returns number of messages published.
        """
        topic = topic or cfg.KAFKA_PRICES_TOPIC
        quotes = self.fetch_with_guard(tickers, target_date)

        for q in quotes:
            key   = f'{q.ticker}:{q.date.isoformat()}'.encode()
            value = json.dumps(q.to_dict()).encode()
            kafka_producer.send(topic, key=key, value=value)

        kafka_producer.flush()
        logger.info('Published %d messages to Kafka topic %s', len(quotes), topic)
        return len(quotes)
