"""Tests for ingestion/yfinance_producer.py — normalization and Snowflake write logic."""

import os
import sys
import unittest
from datetime import date, timedelta
from unittest import mock

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

with mock.patch.dict(os.environ, {'SNOWFLAKE_ACCOUNT': 'T', 'SNOWFLAKE_USER': 'T'}):
    from ingestion.yfinance_producer import YFinanceProducer
    from ingestion.base_producer import DataFetchError, UnifiedQuote


def _make_mock_df(tickers, target_date):
    """Build a minimal yfinance-style MultiIndex DataFrame for testing."""
    import pandas as pd
    import numpy as np

    idx   = pd.DatetimeIndex([pd.Timestamp(target_date)])
    cols  = pd.MultiIndex.from_product(
        [['Open', 'High', 'Low', 'Close', 'Volume'], tickers]
    )
    data  = np.array([[150.0, 155.0, 149.0, 153.0, 5_000_000]] * len(tickers)).T
    # shape: (5 price fields * n_tickers, 1 date)
    arr = np.zeros((len(cols), 1))
    for i, (price, ticker) in enumerate(cols):
        price_vals = {'Open': 150.0, 'High': 155.0, 'Low': 149.0,
                      'Close': 153.0, 'Volume': 5_000_000.0}
        arr[i, 0] = price_vals[price]
    return pd.DataFrame(arr.T, index=idx, columns=cols)


class TestYFinanceProducerNormalize(unittest.TestCase):

    def setUp(self):
        with mock.patch.dict(os.environ, {'SNOWFLAKE_ACCOUNT': 'T', 'SNOWFLAKE_USER': 'T'}):
            self.producer = YFinanceProducer()

    def test_normalize_single_ticker(self):
        tickers     = ['AAPL']
        target_date = date(2025, 4, 9)
        df          = _make_mock_df(tickers, target_date)
        quotes      = self.producer._normalize(df, tickers, target_date, 'test-qid')

        self.assertEqual(len(quotes), 1)
        q = quotes[0]
        self.assertEqual(q.ticker,   'AAPL')
        self.assertEqual(q.date,     target_date)
        self.assertAlmostEqual(q.close, 153.0)
        self.assertAlmostEqual(q.open,  150.0)
        self.assertEqual(q.source,   'yfinance')
        self.assertEqual(q.query_id, 'test-qid')

    def test_normalize_multiple_tickers(self):
        tickers     = ['AAPL', 'MSFT', 'NVDA']
        target_date = date(2025, 4, 9)
        df          = _make_mock_df(tickers, target_date)
        quotes      = self.producer._normalize(df, tickers, target_date, 'qid-multi')

        self.assertEqual(len(quotes), 3)
        returned_tickers = {q.ticker for q in quotes}
        self.assertEqual(returned_tickers, set(tickers))

    def test_missing_ticker_skipped_gracefully(self):
        """If a ticker isn't in the DataFrame, it should be skipped (not crash)."""
        tickers     = ['AAPL']
        target_date = date(2025, 4, 9)
        df          = _make_mock_df(tickers, target_date)
        # Ask for AAPL + TSLA but df only has AAPL
        quotes = self.producer._normalize(df, ['AAPL', 'TSLA'], target_date, 'qid')
        ticker_names = {q.ticker for q in quotes}
        self.assertIn('AAPL', ticker_names)
        self.assertNotIn('TSLA', ticker_names)

    def test_to_dict_roundtrip(self):
        tickers     = ['AAPL']
        target_date = date(2025, 4, 9)
        df          = _make_mock_df(tickers, target_date)
        quotes      = self.producer._normalize(df, tickers, target_date, 'qid-rt')
        d = quotes[0].to_dict()
        self.assertEqual(d['ticker'], 'AAPL')
        self.assertEqual(d['date'],   '2025-04-09')
        self.assertEqual(d['source'], 'yfinance')


class TestYFinanceProducerFetch(unittest.TestCase):
    """Tests that patch yfinance.download to avoid network calls."""

    def setUp(self):
        with mock.patch.dict(os.environ, {'SNOWFLAKE_ACCOUNT': 'T', 'SNOWFLAKE_USER': 'T'}):
            self.producer = YFinanceProducer()

    def test_fetch_returns_unified_quotes(self):
        target_date = date(2025, 4, 9)
        mock_df     = _make_mock_df(['AAPL'], target_date)

        with mock.patch('yfinance.download', return_value=mock_df):
            quotes = self.producer.fetch(['AAPL'], target_date)

        self.assertIsInstance(quotes, list)
        self.assertTrue(all(isinstance(q, UnifiedQuote) for q in quotes))

    def test_fetch_raises_on_empty_df(self):
        import pandas as pd
        empty_df = pd.DataFrame()

        with mock.patch('yfinance.download', return_value=empty_df):
            with self.assertRaises(DataFetchError):
                self.producer.fetch(['AAPL'], date(2025, 4, 9))

    def test_fetch_and_write_to_snowflake(self):
        """fetch_and_write_to_snowflake should call cursor.execute for each quote."""
        target_date = date(2025, 4, 9)
        mock_df     = _make_mock_df(['AAPL', 'MSFT'], target_date)

        mock_cursor = mock.MagicMock()
        mock_conn   = mock.MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        with mock.patch('yfinance.download', return_value=mock_df):
            n = self.producer.fetch_and_write_to_snowflake(
                ['AAPL', 'MSFT'], target_date, mock_conn
            )

        self.assertEqual(n, 2)
        self.assertEqual(mock_cursor.execute.call_count, 2)
        mock_cursor.close.assert_called_once()

    def test_fetch_and_publish_to_kafka(self):
        """fetch_and_publish_to_kafka should call kafka_producer.send for each quote."""
        target_date  = date(2025, 4, 9)
        mock_df      = _make_mock_df(['AAPL'], target_date)
        mock_kafka   = mock.MagicMock()

        with mock.patch('yfinance.download', return_value=mock_df):
            n = self.producer.fetch_and_publish_to_kafka(
                ['AAPL'], target_date, mock_kafka, topic='raw.stock.prices'
            )

        self.assertEqual(n, 1)
        mock_kafka.send.assert_called_once()
        mock_kafka.flush.assert_called_once()

        # Verify key format: b'AAPL:2025-04-09'
        call_kwargs = mock_kafka.send.call_args
        key = call_kwargs[1]['key'] if 'key' in (call_kwargs[1] or {}) else call_kwargs[0][1]
        self.assertIn(b'AAPL', key)


if __name__ == '__main__':
    unittest.main()
