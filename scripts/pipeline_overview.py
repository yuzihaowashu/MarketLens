#!/usr/bin/env python3
"""
Print a read-only snapshot of how MarketLens is configured on this machine.

Usage (from repo root):
    python scripts/pipeline_overview.py
    python scripts/pipeline_overview.py --json

Does not connect to Snowflake or Kafka — safe for onboarding and CI logs.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import config as cfg  # noqa: E402


def _secret_status(val: str | None) -> str:
    if not val:
        return 'not set'
    return 'set (hidden)'


def build_overview_dict() -> dict:
    """Structured overview without leaking secrets."""
    pk_hint = 'from env' if os.environ.get('SNOWFLAKE_PRIVATE_KEY') else 'from path'
    return {
        'snowflake': {
            'account': cfg.SNOWFLAKE_ACCOUNT,
            'user': cfg.SNOWFLAKE_USER,
            'database': cfg.SNOWFLAKE_DATABASE,
            'schema': cfg.SNOWFLAKE_SCHEMA,
            'warehouse': cfg.SNOWFLAKE_WAREHOUSE,
            'role': cfg.SNOWFLAKE_ROLE,
            'private_key': pk_hint,
        },
        'watchlist': list(cfg.WATCHLIST_TICKERS),
        'fred': {'FRED_API_KEY': _secret_status(cfg.FRED_API_KEY)},
        'sec': {'SEC_USER_AGENT': _secret_status(cfg.SEC_USER_AGENT)},
        'kafka': {
            'KAFKA_BOOTSTRAP': cfg.KAFKA_BOOTSTRAP,
            'KAFKA_PRICES_TOPIC': cfg.KAFKA_PRICES_TOPIC,
            'KAFKA_MACRO_TOPIC': cfg.KAFKA_MACRO_TOPIC,
            'KAFKA_SIGNALS_TOPIC': cfg.KAFKA_SIGNALS_TOPIC,
        },
        'notifications': {
            'SLACK_WEBHOOK_URL': _secret_status(cfg.SLACK_WEBHOOK_URL),
            'ALERT_EMAIL': _secret_status(cfg.ALERT_EMAIL),
            'SMTP_USER': _secret_status(cfg.SMTP_USER),
            'SMTP_PASSWORD': _secret_status(cfg.SMTP_PASSWORD),
        },
        'ingestion_tuning': {
            'MAX_WORKERS': cfg.MAX_WORKERS,
            'FETCH_RETRY_ATTEMPTS': cfg.FETCH_RETRY_ATTEMPTS,
            'CIRCUIT_BREAKER_FAILURES': cfg.CIRCUIT_BREAKER_FAILURES,
            'PIPELINE_LOG_ENABLED': cfg.PIPELINE_LOG_ENABLED,
        },
        'repo_paths': {
            'dags_daily_dag': (_ROOT / 'dags' / 'marketlens_daily.py').exists(),
            'reports_extra_metrics': (_ROOT / 'reports' / 'extra_metrics.py').exists(),
            'dbt_project': (_ROOT / 'dbt' / 'dbt_project.yml').exists(),
        },
    }


def _print_human() -> None:
    o = build_overview_dict()
    print('MarketLens — configuration overview (no live connections)\n')
    print('Snowflake')
    for k, v in o['snowflake'].items():
        print(f'  {k}: {v}')
    print('\nWatchlist tickers')
    print(' ', ', '.join(o['watchlist']))
    print('\nFRED / SEC')
    print(' ', o['fred'])
    print(' ', o['sec'])
    print('\nKafka')
    for k, v in o['kafka'].items():
        print(f'  {k}: {v}')
    print('\nNotifications (presence only)')
    for k, v in o['notifications'].items():
        print(f'  {k}: {v}')
    print('\nIngestion / observability')
    for k, v in o['ingestion_tuning'].items():
        print(f'  {k}: {v}')
    print('\nRepo layout hints')
    for k, v in o['repo_paths'].items():
        print(f'  {k}: {v}')
    print('\nTip: read docs/data_journey.md for an end-to-end narrative.')


def main() -> None:
    ap = argparse.ArgumentParser(description='MarketLens config overview (read-only).')
    ap.add_argument('--json', action='store_true', help='Print JSON to stdout.')
    args = ap.parse_args()
    if args.json:
        print(json.dumps(build_overview_dict(), indent=2))
    else:
        _print_human()


if __name__ == '__main__':
    main()
