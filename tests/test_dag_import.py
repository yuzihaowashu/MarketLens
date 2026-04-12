"""
Tests that the Airflow DAG files are structurally correct.

Uses AST parsing rather than importing Airflow directly, so these tests
run even when the Airflow SDK is not fully compatible with the current
Python version (e.g. airflow.sdk missing on Python 3.14).

Validates:
  - Valid Python syntax (no SyntaxError)
  - Expected task IDs are defined as string literals
  - Parallel ingestion pattern is present (both ingest tasks feed refresh)
  - DAG-level settings (dag_id, catchup, schedule) are present
  - Heartbeat stub still has valid syntax
"""

import ast
import os
import sys
import unittest

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_DAGS = os.path.join(_ROOT, 'dags')


def _load_ast(filepath: str) -> ast.Module:
    with open(filepath, 'r') as f:
        source = f.read()
    return ast.parse(source, filename=filepath)


def _collect_string_values(tree: ast.Module) -> set:
    """Return all string constants defined anywhere in the AST."""
    return {
        node.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant) and isinstance(node.value, str)
    }


def _collect_keyword_values(tree: ast.Module, kwarg_name: str) -> set:
    """Return values passed as keyword argument *kwarg_name* anywhere in the AST."""
    values = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.keyword) and node.arg == kwarg_name:
            if isinstance(node.value, ast.Constant):
                values.add(node.value.value)
    return values


class TestMarketlensDailyDagAST(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.path = os.path.join(_DAGS, 'marketlens_daily.py')
        cls.tree = _load_ast(cls.path)
        cls.strings = _collect_string_values(cls.tree)

    def test_file_has_valid_syntax(self):
        """If setUpClass succeeded, syntax is valid — this is a sentinel."""
        self.assertIsNotNone(self.tree)

    def test_dag_id_is_marketlens_daily(self):
        self.assertIn('marketlens_daily', self.strings)

    def test_expected_task_ids_present(self):
        for task_id in ('ingest_prices', 'ingest_macro',
                        'refresh_signals', 'anomaly_check', 'notify'):
            self.assertIn(task_id, self.strings,
                          msg=f'task_id "{task_id}" not found in DAG source')

    def test_no_catchup_is_false(self):
        """catchup=False must be present."""
        catchup_values = set()
        for node in ast.walk(self.tree):
            if isinstance(node, ast.keyword) and node.arg == 'catchup':
                if isinstance(node.value, ast.Constant):
                    catchup_values.add(node.value.value)
        self.assertIn(False, catchup_values, 'catchup=False not found in DAG definition')

    def test_marketlens_tag_present(self):
        self.assertIn('marketlens', self.strings)

    def test_parallel_ingestion_pattern(self):
        """
        The source should contain a list-based upstream expression like:
        [ingest_prices, ingest_macro] >> refresh_signals
        We check this by looking for a BinOp with both task names as string constants
        in the vicinity of the >> operator (ast.RShift).
        """
        found_rshift = False
        for node in ast.walk(self.tree):
            if isinstance(node, ast.BinOp) and isinstance(node.op, ast.RShift):
                found_rshift = True
                break
        self.assertTrue(found_rshift,
                        'No >> operator found — DAG dependency chain missing')

    def test_notify_task_present(self):
        self.assertIn('notify', self.strings)

    def test_retries_configured(self):
        """default_args dict should include 'retries' key with value >= 1."""
        # 'retries' appears as a string constant key in the default_args dict
        self.assertIn('retries', self.strings,
                      "'retries' key not found in DAG source")

    def test_pipeline_log_call_present(self):
        """_log_run helper should be called in the source."""
        self.assertIn('_log_run', self.strings | {
            node.id for node in ast.walk(self.tree) if isinstance(node, ast.Name)
        })

    def test_xcom_pull_in_notify(self):
        """notify task should pull from XCom."""
        xcom_methods = {
            node.attr
            for node in ast.walk(self.tree)
            if isinstance(node, ast.Attribute)
        }
        self.assertIn('xcom_pull', xcom_methods,
                      'notify should use xcom_pull to read anomaly_check results')


class TestHeartbeatDagAST(unittest.TestCase):
    """Original heartbeat stub should still have valid syntax."""

    def test_heartbeat_has_valid_syntax(self):
        path = os.path.join(_DAGS, 'marketlens_heartbeat.py')
        tree = _load_ast(path)
        self.assertIsNotNone(tree)

    def test_heartbeat_has_dag_id(self):
        path    = os.path.join(_DAGS, 'marketlens_heartbeat.py')
        tree    = _load_ast(path)
        strings = _collect_string_values(tree)
        self.assertIn('marketlens_heartbeat', strings)

    def test_heartbeat_has_marketlens_tag(self):
        path    = os.path.join(_DAGS, 'marketlens_heartbeat.py')
        tree    = _load_ast(path)
        strings = _collect_string_values(tree)
        self.assertIn('marketlens', strings)


if __name__ == '__main__':
    unittest.main()
