"""MarketLens ingestion layer — data producers and Kafka consumer."""
from ingestion.base_producer import BaseProducer, UnifiedQuote, DataFetchError
from ingestion.yfinance_producer import YFinanceProducer
from ingestion.macro_producer import MacroProducer, MacroIndicator

__all__ = [
    'BaseProducer', 'UnifiedQuote', 'DataFetchError',
    'YFinanceProducer',
    'MacroProducer', 'MacroIndicator',
]
