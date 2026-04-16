"""Smoke tests for scripts/pipeline_overview.build_overview_dict."""

from __future__ import annotations

import os
import sys
import unittest

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from scripts.pipeline_overview import build_overview_dict


class TestPipelineOverview(unittest.TestCase):

    def test_overview_has_expected_sections(self):
        d = build_overview_dict()
        self.assertIn('snowflake', d)
        self.assertIn('watchlist', d)
        self.assertIn('kafka', d)
        self.assertIn('repo_paths', d)
        self.assertIsInstance(d['watchlist'], list)
        self.assertTrue(d['repo_paths'].get('dags_daily_dag'))

    def test_notification_fields_are_status_only(self):
        d = build_overview_dict()
        for _k, v in d['notifications'].items():
            self.assertIn(v, ('set (hidden)', 'not set'))


if __name__ == '__main__':
    unittest.main()
