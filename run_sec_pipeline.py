"""
Manual SEC EDGAR pipeline runner — bypass Airflow.

Runs all three stages end-to-end for the watchlist:
  1. fetch_filing_metadata  → RAW_SEC_FILINGS
  2. fetch_filing_text      → RAW_SEC_FILING_TEXT
  3. summarize_filings      → SEC_FILING_SUMMARIES

Usage:
    .venv/bin/python run_sec_pipeline.py
    .venv/bin/python run_sec_pipeline.py --tickers AAPL,MSFT --limit 5
"""

from __future__ import annotations

import argparse
import logging
import sys

import config as cfg
from app.snowflake_client import get_connection
from ingestion.sec_producer import SECProducer


def main():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(name)s: %(message)s',
    )
    log = logging.getLogger('sec_pipeline')

    parser = argparse.ArgumentParser()
    parser.add_argument('--tickers', default=','.join(cfg.WATCHLIST_TICKERS),
                        help='Comma-separated tickers (default: watchlist)')
    parser.add_argument('--limit', type=int, default=cfg.SEC_MAX_FILINGS_PER_RUN,
                        help='Max filings per batch (default: SEC_MAX_FILINGS_PER_RUN)')
    parser.add_argument('--all', action='store_true',
                        help='Loop stages 2+3 until the backlog is drained')
    args = parser.parse_args()

    if not cfg.SEC_USER_AGENT:
        log.error('SEC_USER_AGENT is not set. Add it to .env:')
        log.error('  SEC_USER_AGENT="Your Name your-email@example.com"')
        sys.exit(1)

    tickers = [t.strip().upper() for t in args.tickers.split(',') if t.strip()]
    log.info('Tickers: %s  limit=%d', tickers, args.limit)

    conn = get_connection()
    producer = SECProducer()

    log.info('=== Stage 1: metadata ===')
    n_meta = producer.fetch_filing_metadata(tickers, conn)
    log.info('Stage 1 done: %d filing rows merged', n_meta)

    total_text = 0
    total_sum = 0
    batch = 0
    while True:
        batch += 1
        log.info('=== Batch %d: stage 2 (text) ===', batch)
        n_text = producer.fetch_filing_text(conn, max_filings=args.limit)
        total_text += n_text
        log.info('Batch %d stage 2: %d chunks', batch, n_text)

        log.info('=== Batch %d: stage 3 (summaries) ===', batch)
        n_sum = producer.summarize_filings(conn, max_filings=args.limit)
        total_sum += n_sum
        log.info('Batch %d stage 3: %d summaries', batch, n_sum)

        if not args.all:
            break
        if n_text == 0 and n_sum == 0:
            log.info('Backlog drained — exiting loop')
            break

    log.info('Pipeline complete. totals: text_chunks=%d summaries=%d batches=%d',
             total_text, total_sum, batch)


if __name__ == '__main__':
    main()
