"""Tests for ingestion/fred_producer.py — FRED API fetch + Snowflake MERGE."""

import os
import sys
import unittest
from unittest import mock

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

with mock.patch.dict(os.environ, {'SNOWFLAKE_ACCOUNT': 'T', 'SNOWFLAKE_USER': 'T',
                                  'FRED_API_KEY': 'test-key'}):
    import config as cfg
    # Re-read FRED_API_KEY since config is imported once at module scope
    cfg.FRED_API_KEY = 'test-key'
    from ingestion.fred_producer import FredProducer
    from ingestion.macro_producer import MacroIndicator


def _mock_response(observations):
    r = mock.MagicMock()
    r.raise_for_status = mock.MagicMock()
    r.json = mock.MagicMock(return_value={'observations': observations})
    return r


class TestFredProducerFetchSeries(unittest.TestCase):

    def setUp(self):
        self.producer = FredProducer(api_key='test-key')

    def test_parses_happy_path(self):
        obs = [
            {'date': '2025-01-01', 'value': '100.5'},
            {'date': '2025-02-01', 'value': '101.2'},
        ]
        with mock.patch.object(self.producer.session, 'get',
                               return_value=_mock_response(obs)):
            rows = self.producer._fetch_series('GDPC1', 'GDP_REAL')

        self.assertEqual(len(rows), 2)
        self.assertTrue(all(isinstance(r, MacroIndicator) for r in rows))
        self.assertEqual(rows[0].variable, 'GDP_REAL')
        self.assertEqual(rows[0].source, 'fred')
        self.assertEqual(rows[0].geo_id, 'country/USA')
        self.assertAlmostEqual(rows[0].value, 100.5)

    def test_skips_missing_marker(self):
        obs = [
            {'date': '2025-01-01', 'value': '.'},
            {'date': '2025-02-01', 'value': '42.0'},
            {'date': '2025-03-01', 'value': '.'},
        ]
        with mock.patch.object(self.producer.session, 'get',
                               return_value=_mock_response(obs)):
            rows = self.producer._fetch_series('HOUST', 'HOUSING_STARTS')

        self.assertEqual(len(rows), 1)
        self.assertAlmostEqual(rows[0].value, 42.0)

    def test_raises_without_api_key(self):
        p = FredProducer(api_key=None)
        p.api_key = None
        with self.assertRaises(RuntimeError):
            p._fetch_series('GDPC1', 'GDP_REAL')


class TestFredProducerFetchAll(unittest.TestCase):

    def test_one_failing_series_does_not_kill_others(self):
        producer = FredProducer(api_key='test-key')

        good = _mock_response([{'date': '2025-01-01', 'value': '1.0'}])

        def fake_get(url, params=None, timeout=None):
            if params.get('series_id') == cfg.FRED_SERIES_HOUSING_STARTS:
                raise RuntimeError('boom')
            return good

        with mock.patch.object(producer.session, 'get', side_effect=fake_get):
            rows = producer.fetch_all()

        # 3 successful series × 1 row each = 3
        self.assertEqual(len(rows), 3)
        variables = {r.variable for r in rows}
        self.assertNotIn('HOUSING_STARTS', variables)


class TestFredProducerWrite(unittest.TestCase):

    def test_merge_writes_expected_row_count(self):
        producer = FredProducer(api_key='test-key')

        resp = _mock_response([{'date': '2025-01-01', 'value': '10.0'}])
        mock_cursor = mock.MagicMock()
        mock_conn   = mock.MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        with mock.patch.object(producer.session, 'get', return_value=resp), \
             mock.patch('snowflake.connector.pandas_tools.write_pandas',
                        return_value=(True, 1, 4, None)) as mock_wp:
            n = producer.fetch_and_write_to_snowflake(mock_conn)

        # 4 series × 1 row each
        self.assertEqual(n, 4)
        # One CREATE TEMP TABLE + one MERGE = 2 execute calls; bulk load is via write_pandas
        self.assertEqual(mock_cursor.execute.call_count, 2)
        mock_wp.assert_called_once()
        mock_cursor.close.assert_called_once()

    def test_empty_api_key_returns_zero(self):
        producer = FredProducer(api_key=None)
        producer.api_key = None
        mock_conn = mock.MagicMock()
        n = producer.fetch_and_write_to_snowflake(mock_conn)
        self.assertEqual(n, 0)
        mock_conn.cursor.assert_not_called()


if __name__ == '__main__':
    unittest.main()
