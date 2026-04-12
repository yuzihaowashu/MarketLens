"""Tests for config.py parsing helpers."""

import os
import sys
import unittest

# Ensure project root is on path before importing config
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

# Stub out dotenv so tests don't require a .env file
import unittest.mock as mock
with mock.patch.dict(os.environ, {
    'SNOWFLAKE_ACCOUNT': 'TEST-ACCOUNT',
    'SNOWFLAKE_USER':    'TEST_USER',
}):
    import config


class TestParseEnvBool(unittest.TestCase):

    def test_true_values(self):
        for val in ('1', 'true', 'True', 'TRUE', 'yes', 'on'):
            with mock.patch.dict(os.environ, {'TEST_BOOL': val}):
                self.assertTrue(config.parse_env_bool('TEST_BOOL', False),
                                msg=f'Expected True for "{val}"')

    def test_false_values(self):
        for val in ('0', 'false', 'False', 'FALSE', 'no', 'off'):
            with mock.patch.dict(os.environ, {'TEST_BOOL': val}):
                self.assertFalse(config.parse_env_bool('TEST_BOOL', True),
                                 msg=f'Expected False for "{val}"')

    def test_missing_key_returns_default_true(self):
        env = {k: v for k, v in os.environ.items() if k != 'TEST_BOOL'}
        with mock.patch.dict(os.environ, env, clear=True):
            self.assertTrue(config.parse_env_bool('TEST_BOOL', True))

    def test_missing_key_returns_default_false(self):
        env = {k: v for k, v in os.environ.items() if k != 'TEST_BOOL'}
        with mock.patch.dict(os.environ, env, clear=True):
            self.assertFalse(config.parse_env_bool('TEST_BOOL', False))

    def test_unrecognised_value_returns_default(self):
        with mock.patch.dict(os.environ, {'TEST_BOOL': 'maybe'}):
            self.assertTrue(config.parse_env_bool('TEST_BOOL', True))
            self.assertFalse(config.parse_env_bool('TEST_BOOL', False))


class TestParseEnvInt(unittest.TestCase):

    def test_valid_int(self):
        with mock.patch.dict(os.environ, {'TEST_INT': '8'}):
            self.assertEqual(config.parse_env_int('TEST_INT', 4), 8)

    def test_default_when_missing(self):
        env = {k: v for k, v in os.environ.items() if k != 'TEST_INT'}
        with mock.patch.dict(os.environ, env, clear=True):
            self.assertEqual(config.parse_env_int('TEST_INT', 4), 4)

    def test_default_on_invalid_value(self):
        with mock.patch.dict(os.environ, {'TEST_INT': 'abc'}):
            self.assertEqual(config.parse_env_int('TEST_INT', 4), 4)

    def test_min_clamping(self):
        with mock.patch.dict(os.environ, {'TEST_INT': '0'}):
            self.assertEqual(config.parse_env_int('TEST_INT', 4, min_val=1), 1)

    def test_max_clamping(self):
        with mock.patch.dict(os.environ, {'TEST_INT': '100'}):
            self.assertEqual(config.parse_env_int('TEST_INT', 4, max_val=16), 16)

    def test_within_bounds_unchanged(self):
        with mock.patch.dict(os.environ, {'TEST_INT': '8'}):
            self.assertEqual(config.parse_env_int('TEST_INT', 4, min_val=1, max_val=16), 8)


class TestParseEnvList(unittest.TestCase):

    def test_comma_separated(self):
        with mock.patch.dict(os.environ, {'TEST_LIST': 'AAPL, MSFT, GOOGL'}):
            self.assertEqual(config.parse_env_list('TEST_LIST', []), ['AAPL', 'MSFT', 'GOOGL'])

    def test_single_item(self):
        with mock.patch.dict(os.environ, {'TEST_LIST': 'AAPL'}):
            self.assertEqual(config.parse_env_list('TEST_LIST', []), ['AAPL'])

    def test_empty_returns_default(self):
        with mock.patch.dict(os.environ, {'TEST_LIST': ''}):
            self.assertEqual(config.parse_env_list('TEST_LIST', ['SPY']), ['SPY'])

    def test_missing_returns_default(self):
        env = {k: v for k, v in os.environ.items() if k != 'TEST_LIST'}
        with mock.patch.dict(os.environ, env, clear=True):
            self.assertEqual(config.parse_env_list('TEST_LIST', ['SPY']), ['SPY'])

    def test_strips_whitespace(self):
        with mock.patch.dict(os.environ, {'TEST_LIST': '  AAPL ,  TSLA  '}):
            self.assertEqual(config.parse_env_list('TEST_LIST', []), ['AAPL', 'TSLA'])

    def test_skips_empty_segments(self):
        with mock.patch.dict(os.environ, {'TEST_LIST': 'AAPL,,MSFT,'}):
            self.assertEqual(config.parse_env_list('TEST_LIST', []), ['AAPL', 'MSFT'])


if __name__ == '__main__':
    unittest.main()
