"""
Manual daily ingest runner — FRED + Prices + Macro (bypasses Airflow).

Runs the three non-SEC producers in sequence:

  1. YFinanceProducer → RAW_STOCK_PRICES
        Source: Yahoo Finance (yfinance library) — free public endpoint.
        Pulls daily OHLCV for config.WATCHLIST_TICKERS
        (AAPL, MSFT, GOOGL, AMZN, TSLA, NVDA, META, SPY, QQQ).

  2. MacroProducer    → RAW_MACRO_INDICATORS
        Source: Snowflake Marketplace share `SNOWFLAKE_PUBLIC_DATA_FREE`
        (Snowflake → Snowflake query, no external HTTP).
        Pulls Fed Funds Rate (EFFR_PCT, EFFR_TARGET_RATE_TP) and
        CPI (All Items, Seasonally Adjusted). If SNOWFLAKE_PAID_DATA_AVAILABLE
        is true, also pulls 10Y Treasury + Unemployment.

  3. FredProducer     → RAW_FRED_INDICATORS
        Source: FRED API (St. Louis Fed) — https://api.stlouisfed.org
        Pulls GDPC1 (Real GDP), HOUST (Housing Starts),
        UMCSENT (Consumer Sentiment), T10YIE (10Y Breakeven Inflation).
        Requires FRED_API_KEY in .env.

Usage:
    .venv/bin/python run_fred_ingest.py
    .venv/bin/python run_fred_ingest.py --date 2026-04-10
"""

from __future__ import annotations

import argparse
import logging
import sys
import uuid
from datetime import date, datetime

sys.path.insert(0, '.')
sys.path.insert(0, 'app')

import config as cfg
from snowflake_client import get_connection
from ingestion.yfinance_producer import YFinanceProducer
from ingestion.macro_producer import MacroProducer
from ingestion.fred_producer import FredProducer
from ingestion.pipeline_logger import log_run


def _run_task(run_id: str, task_id: str, fn):
    """Run a producer function and log start/complete/failed to PIPELINE_RUN_LOG."""
    started_at = datetime.utcnow()
    log_run(run_id, task_id, 'started', started_at=started_at)
    try:
        n = fn()
        log_run(run_id, task_id, 'completed',
                row_count=n, started_at=started_at, completed_at=datetime.utcnow())
        return n
    except Exception as exc:
        log_run(run_id, task_id, 'failed',
                error_msg=str(exc)[:500],
                started_at=started_at, completed_at=datetime.utcnow())
        raise


def main():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s: %(message)s',
    )
    log = logging.getLogger('daily_ingest')

    parser = argparse.ArgumentParser()
    parser.add_argument('--date', default=None,
                        help='Target date YYYY-MM-DD (default: today)')
    args = parser.parse_args()

    target = date.fromisoformat(args.date) if args.date else date.today()
    log.info('Target date: %s', target)

    run_id = uuid.uuid4().hex
    conn = get_connection()

    log.info('=== ingest_prices ===')
    n = _run_task(run_id, 'ingest_prices',
                  lambda: YFinanceProducer().fetch_and_write_to_snowflake(cfg.WATCHLIST_TICKERS, target, conn))
    log.info('ingest_prices: %d rows', n)

    log.info('=== ingest_macro ===')
    n = _run_task(run_id, 'ingest_macro',
                  lambda: MacroProducer().fetch_and_write_to_snowflake(conn))
    log.info('ingest_macro: %d rows', n)

    log.info('=== ingest_fred ===')
    if not cfg.FRED_API_KEY:
        log.info('ingest_fred: skipped (FRED_API_KEY not set)')
    else:
        n = _run_task(run_id, 'ingest_fred',
                      lambda: FredProducer().fetch_and_write_to_snowflake(conn))
        log.info('ingest_fred: %d rows', n)

    log.info('Daily ingest complete.')


if __name__ == '__main__':
    main()
