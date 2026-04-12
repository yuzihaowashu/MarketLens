"""Tests for ingestion/base_producer.py — UnifiedQuote, circuit breaker, fallback chain."""

import os
import sys
import time
import unittest
from datetime import date
from unittest import mock

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

# Patch dotenv before importing anything that touches config
with mock.patch.dict(os.environ, {'SNOWFLAKE_ACCOUNT': 'T', 'SNOWFLAKE_USER': 'T'}):
    from ingestion.base_producer import (
        BaseProducer, UnifiedQuote, DataFetchError, run_with_fallback
    )


# ---------------------------------------------------------------------------
# Concrete stub producers for testing
# ---------------------------------------------------------------------------

class AlwaysSuccessProducer(BaseProducer):
    name     = 'success'
    priority = 1

    def fetch(self, tickers, target_date):
        return [UnifiedQuote(ticker=t, date=target_date,
                             open=1.0, high=2.0, low=0.5, close=1.5,
                             volume=1000, source=self.name)
                for t in tickers]


class AlwaysFailProducer(BaseProducer):
    name     = 'fail'
    priority = 2

    def fetch(self, tickers, target_date):
        raise DataFetchError('always fails')


class CountingProducer(BaseProducer):
    """Fails for the first N calls, then succeeds."""
    name     = 'counting'
    priority = 1

    def __init__(self, fail_count: int, **kw):
        super().__init__(**kw)
        self._fail_count = fail_count
        self.calls = 0

    def fetch(self, tickers, target_date):
        self.calls += 1
        if self.calls <= self._fail_count:
            raise DataFetchError(f'fail #{self.calls}')
        return [UnifiedQuote(ticker=tickers[0], date=target_date,
                             open=1.0, high=1.0, low=1.0, close=1.0,
                             volume=100, source=self.name)]


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestUnifiedQuote(unittest.TestCase):

    def test_to_dict_has_all_fields(self):
        q = UnifiedQuote(ticker='AAPL', date=date(2025, 1, 10),
                         open=150.0, high=155.0, low=149.0, close=153.0,
                         volume=5_000_000, source='yfinance')
        d = q.to_dict()
        for key in ('ticker', 'date', 'open', 'high', 'low', 'close', 'volume', 'source', 'query_id'):
            self.assertIn(key, d)

    def test_date_serialised_as_iso_string(self):
        q = UnifiedQuote(ticker='AAPL', date=date(2025, 1, 10),
                         open=None, high=None, low=None, close=None,
                         volume=None, source='test')
        self.assertEqual(q.to_dict()['date'], '2025-01-10')

    def test_query_id_auto_generated(self):
        q1 = UnifiedQuote(ticker='AAPL', date=date(2025, 1, 1),
                          open=1.0, high=1.0, low=1.0, close=1.0,
                          volume=1, source='test')
        q2 = UnifiedQuote(ticker='AAPL', date=date(2025, 1, 1),
                          open=1.0, high=1.0, low=1.0, close=1.0,
                          volume=1, source='test')
        self.assertNotEqual(q1.query_id, q2.query_id)


class TestCircuitBreaker(unittest.TestCase):

    def test_available_initially(self):
        p = AlwaysFailProducer()
        self.assertTrue(p.is_available())

    def test_enters_cooldown_after_threshold(self):
        p = AlwaysFailProducer(failure_threshold=3, cooldown_seconds=60)
        for _ in range(3):
            p.record_failure()
        self.assertFalse(p.is_available())

    def test_success_resets_counter(self):
        p = AlwaysFailProducer(failure_threshold=3, cooldown_seconds=60)
        p.record_failure()
        p.record_failure()
        p.record_success()
        # After reset, 3 more failures should be needed to enter cooldown
        p.record_failure()
        p.record_failure()
        self.assertTrue(p.is_available())   # only 2, not yet at threshold
        p.record_failure()
        self.assertFalse(p.is_available())  # now at threshold

    def test_fetch_with_guard_raises_when_in_cooldown(self):
        p = AlwaysFailProducer(failure_threshold=1, cooldown_seconds=300)
        # Trigger cooldown
        with self.assertRaises(DataFetchError):
            p.fetch_with_guard(['AAPL'], date(2025, 1, 1))
        # Should now be in cooldown
        self.assertFalse(p.is_available())
        with self.assertRaises(RuntimeError):
            p.fetch_with_guard(['AAPL'], date(2025, 1, 1))

    def test_cooldown_expires(self):
        p = AlwaysFailProducer(failure_threshold=1, cooldown_seconds=1)
        try:
            p.fetch_with_guard(['AAPL'], date(2025, 1, 1))
        except Exception:
            pass
        self.assertFalse(p.is_available())
        # Manually expire cooldown
        p._cooldown_until = time.time() - 1
        self.assertTrue(p.is_available())


class TestRunWithFallback(unittest.TestCase):

    def test_primary_success_used(self):
        primary  = AlwaysSuccessProducer()   # priority=1 (class attr)
        fallback = AlwaysFailProducer()      # priority=2 (class attr)
        today    = date(2025, 4, 1)
        quotes   = run_with_fallback([primary, fallback], ['AAPL'], today)
        self.assertEqual(len(quotes), 1)
        self.assertEqual(quotes[0].source, 'success')

    def test_falls_back_to_secondary(self):
        failing = AlwaysFailProducer()       # priority=2
        success = AlwaysSuccessProducer()    # priority=1 — still wins on sort
        # Override to force failure-first ordering for this test
        failing.priority = 1
        success.priority = 2
        today   = date(2025, 4, 1)
        quotes  = run_with_fallback([failing, success], ['AAPL'], today)
        self.assertEqual(quotes[0].source, 'success')

    def test_all_fail_raises(self):
        p1 = AlwaysFailProducer()
        p2 = AlwaysFailProducer()
        with self.assertRaises(DataFetchError):
            run_with_fallback([p1, p2], ['AAPL'], date(2025, 4, 1))

    def test_sorted_by_priority(self):
        """Producers should be tried in ascending priority order."""
        called_order = []

        class TrackingProducer(BaseProducer):
            def __init__(self, name, prio):
                super().__init__()
                self.name     = name
                self.priority = prio

            def fetch(self, tickers, target_date):
                called_order.append(self.name)
                raise DataFetchError('fail')

        producers = [TrackingProducer('p3', 3), TrackingProducer('p1', 1),
                     TrackingProducer('p2', 2)]
        with self.assertRaises(DataFetchError):
            run_with_fallback(producers, ['AAPL'], date(2025, 1, 1))

        self.assertEqual(called_order, ['p1', 'p2', 'p3'])


if __name__ == '__main__':
    unittest.main()
