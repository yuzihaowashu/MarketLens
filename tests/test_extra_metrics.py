"""Contract tests for optional ``reports.extra_metrics`` extensions."""

from __future__ import annotations

import os
import sys
import unittest
from datetime import date

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from reports import extra_metrics as em


class TestExtraMetricsContract(unittest.TestCase):
    """Stubs return empty lists of MetricRow; implementations must keep types."""

    def test_watchlist_breadth_returns_list_of_metric_rows(self):
        out = em.watchlist_breadth_from_daily_returns({'AAPL': 0.5, 'MSFT': -0.2})
        self.assertIsInstance(out, list)
        self.assertTrue(all(isinstance(x, em.MetricRow) for x in out))

    def test_fred_macro_spread_returns_list_of_metric_rows(self):
        out = em.fred_macro_spread_metrics({'TREASURY_10Y': 4.2})
        self.assertIsInstance(out, list)
        self.assertTrue(all(isinstance(x, em.MetricRow) for x in out))

    def test_liquidity_proxy_returns_list_of_metric_rows(self):
        out = em.liquidity_proxy_from_volumes(
            {'AAPL': 1e8},
            {'AAPL': 8e7},
        )
        self.assertIsInstance(out, list)
        self.assertTrue(all(isinstance(x, em.MetricRow) for x in out))

    def test_term_structure_returns_list_of_metric_rows(self):
        d = date(2026, 1, 2)
        out = em.term_structure_kink_signal([(d, 4.0)], [(d, 4.5)])
        self.assertIsInstance(out, list)
        self.assertTrue(all(isinstance(x, em.MetricRow) for x in out))

    def test_metric_row_is_frozen_dataclass(self):
        row = em.MetricRow(
            metric_id='test',
            label='Test',
            value=1.0,
            interpretation='demo',
        )
        self.assertEqual(row.metric_id, 'test')
        with self.assertRaises(AttributeError):
            row.value = 2.0  # type: ignore[misc]


if __name__ == '__main__':
    unittest.main()
