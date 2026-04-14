"""
Static ticker → CIK mapping for the MarketLens watchlist.

EDGAR publishes the full mapping at https://www.sec.gov/files/company_tickers.json,
but the watchlist is small and stable, so we hard-code it to avoid an extra HTTP
dependency. Extend this dict when adding tickers to config.WATCHLIST_TICKERS.

CIKs must be 10 digits, zero-padded (EDGAR's submissions endpoint requires this).
ETFs like SPY/QQQ do not file 10-K/10-Q — they're excluded here; the producer
skips tickers missing from this map.
"""

from __future__ import annotations

TICKER_TO_CIK: dict[str, str] = {
    'AAPL':  '0000320193',
    'MSFT':  '0000789019',
    'GOOGL': '0001652044',
    'AMZN':  '0001018724',
    'TSLA':  '0001318605',
    'NVDA':  '0001045810',
    'META':  '0001326801',
}


def cik_for(ticker: str) -> str | None:
    return TICKER_TO_CIK.get(ticker.upper())
