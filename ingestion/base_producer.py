"""
Base producer abstraction for all MarketLens data fetchers.

Inspired by daily_stock_analysis/data_provider/base.py:
- Priority-ordered fallback chain
- Circuit breaker: cooldown after repeated failures
- UnifiedQuote: normalized schema regardless of source
"""

from __future__ import annotations

import logging
import time
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import date
from typing import List, Optional

logger = logging.getLogger(__name__)


class DataFetchError(Exception):
    """Raised when a producer cannot retrieve data."""


@dataclass
class UnifiedQuote:
    """
    Normalized market quote — one row per ticker per day.

    All producers emit this shape so consumers (Snowflake writer, Kafka
    publisher) are source-agnostic.
    """
    ticker:     str
    date:       date
    open:       Optional[float]
    high:       Optional[float]
    low:        Optional[float]
    close:      Optional[float]
    volume:     Optional[int]
    source:     str               # 'yfinance', 'alpaca', etc.
    query_id:   str = field(default_factory=lambda: str(uuid.uuid4()))

    def to_dict(self) -> dict:
        return {
            'ticker':    self.ticker,
            'date':      self.date.isoformat(),
            'open':      self.open,
            'high':      self.high,
            'low':       self.low,
            'close':     self.close,
            'volume':    self.volume,
            'source':    self.source,
            'query_id':  self.query_id,
        }


class BaseProducer(ABC):
    """
    Abstract base for all data producers.

    Subclasses implement fetch().  The circuit breaker (is_available /
    record_failure / record_success) prevents a broken source from
    stalling every DAG run.
    """

    name:     str = 'base'
    priority: int = 99          # lower = higher priority

    def __init__(self,
                 failure_threshold: int = 3,
                 cooldown_seconds: int = 300):
        self._failure_count:  int = 0
        self._cooldown_until: Optional[float] = None
        self._failure_threshold = failure_threshold
        self._cooldown_seconds  = cooldown_seconds

    # ------------------------------------------------------------------
    # Circuit breaker
    # ------------------------------------------------------------------

    def is_available(self) -> bool:
        """Return False if this source is currently in cooldown."""
        if self._cooldown_until is not None and time.time() < self._cooldown_until:
            remaining = int(self._cooldown_until - time.time())
            logger.debug('%s in cooldown for %ds more', self.name, remaining)
            return False
        return True

    def record_failure(self) -> None:
        """Increment failure counter; enter cooldown after threshold."""
        self._failure_count += 1
        logger.warning('%s failure count: %d/%d',
                       self.name, self._failure_count, self._failure_threshold)
        if self._failure_count >= self._failure_threshold:
            self._cooldown_until = time.time() + self._cooldown_seconds
            self._failure_count = 0
            logger.warning('%s entering %ds cooldown', self.name, self._cooldown_seconds)

    def record_success(self) -> None:
        """Reset failure counter and lift cooldown."""
        self._failure_count = 0
        self._cooldown_until = None

    # ------------------------------------------------------------------
    # Abstract interface
    # ------------------------------------------------------------------

    @abstractmethod
    def fetch(self, tickers: List[str], target_date: date) -> List[UnifiedQuote]:
        """
        Fetch daily OHLCV quotes for *tickers* on *target_date*.

        Raises DataFetchError on failure.  Must NOT swallow exceptions —
        the circuit breaker wrapper handles that.
        """

    # ------------------------------------------------------------------
    # Guarded entry point
    # ------------------------------------------------------------------

    def fetch_with_guard(self, tickers: List[str], target_date: date) -> List[UnifiedQuote]:
        """
        Calls fetch() wrapped with circuit-breaker logic.

        Raises RuntimeError if the source is in cooldown.
        Raises DataFetchError (or any exception) from fetch() after
        recording the failure.
        """
        if not self.is_available():
            raise RuntimeError(
                f'{self.name} is in cooldown — skipping until '
                f'{time.strftime("%H:%M:%S", time.localtime(self._cooldown_until))}'
            )
        try:
            quotes = self.fetch(tickers, target_date)
            self.record_success()
            logger.info('%s fetched %d quotes for %s', self.name, len(quotes), target_date)
            return quotes
        except Exception:
            self.record_failure()
            raise


def run_with_fallback(producers: List[BaseProducer],
                      tickers: List[str],
                      target_date: date) -> List[UnifiedQuote]:
    """
    Try producers in priority order (lowest priority number first).
    Returns results from the first producer that succeeds.

    Inspired by daily_stock_analysis's multi-source fallback chain.
    """
    sorted_producers = sorted(producers, key=lambda p: p.priority)
    last_error: Optional[Exception] = None

    for producer in sorted_producers:
        if not producer.is_available():
            continue
        try:
            return producer.fetch_with_guard(tickers, target_date)
        except Exception as exc:
            logger.warning('Producer %s failed: %s — trying next', producer.name, exc)
            last_error = exc

    raise DataFetchError(
        f'All producers failed for {target_date}. '
        f'Last error: {last_error}'
    )
