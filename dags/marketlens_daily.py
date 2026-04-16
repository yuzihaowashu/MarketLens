"""
MarketLens daily pipeline DAG.

Replaces the marketlens_heartbeat stub with a real end-to-end pipeline:

    [ingest_prices]─┐
                    ├──► [refresh_signals] ──► [anomaly_check] ──► [notify]
    [ingest_macro]──┘

Schedule: 18:00 ET on weekdays (after US market close).
Manually triggered runs always process the logical execution date.

Phase 1  (current): producers write directly to Snowflake.
Phase 2  (Kafka):   swap ingest tasks to use fetch_and_publish_to_kafka +
                    a consume_to_snowflake task between ingestion and signals.
"""

from __future__ import annotations

import logging
import os
import sys
import uuid
from datetime import date, datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# ---------------------------------------------------------------------------
# Make project root importable in Airflow's execution environment
# ---------------------------------------------------------------------------
_DAG_DIR  = os.path.dirname(os.path.abspath(__file__))
_ROOT_DIR = os.path.dirname(_DAG_DIR)
for _p in (_ROOT_DIR, os.path.join(_ROOT_DIR, 'app')):
    if _p not in sys.path:
        sys.path.insert(0, _p)

logger = logging.getLogger(__name__)

from ingestion.pipeline_logger import log_run as _log_run_shared


# ---------------------------------------------------------------------------
# Task callables
# ---------------------------------------------------------------------------

def _log_run(run_id: str, task_id: str, status: str,
             row_count: int = 0, error_msg: str = '',
             started_at: datetime = None, completed_at: datetime = None):
    """Write a row to PIPELINE_RUN_LOG (delegates to shared pipeline_logger)."""
    _log_run_shared(run_id, task_id, status,
                    row_count=row_count, error_msg=error_msg,
                    started_at=started_at, completed_at=completed_at)


def _ingest_prices(**ctx):
    """
    Task 1a: Fetch daily OHLCV from YFinance → MERGE into RAW_STOCK_PRICES.

    Phase 2 swap: replace fetch_and_write_to_snowflake with
    fetch_and_publish_to_kafka to route through Kafka instead.
    """
    import config as cfg
    from snowflake_client import get_connection
    from ingestion.yfinance_producer import YFinanceProducer

    run_id     = ctx['run_id']
    target_str = ctx['ds']                                 # YYYY-MM-DD string
    target     = date.fromisoformat(target_str)

    started_at = datetime.utcnow()
    _log_run(run_id, 'ingest_prices', 'started', started_at=started_at)

    try:
        conn     = get_connection()
        producer = YFinanceProducer()
        n = producer.fetch_and_write_to_snowflake(cfg.WATCHLIST_TICKERS, target, conn)
        logger.info('ingest_prices: wrote %d rows for %s', n, target_str)
        _log_run(run_id, 'ingest_prices', 'completed',
                 row_count=n, started_at=started_at, completed_at=datetime.utcnow())
        return n
    except Exception as exc:
        _log_run(run_id, 'ingest_prices', 'failed',
                 error_msg=str(exc)[:500],
                 started_at=started_at, completed_at=datetime.utcnow())
        raise


def _ingest_macro(**ctx):
    """
    Task 1b: Fetch macro indicators → MERGE into RAW_MACRO_INDICATORS.
    Runs in parallel with _ingest_prices.
    """
    from snowflake_client import get_connection
    from ingestion.macro_producer import MacroProducer

    run_id     = ctx['run_id']
    started_at = datetime.utcnow()
    _log_run(run_id, 'ingest_macro', 'started', started_at=started_at)

    try:
        conn = get_connection()
        n    = MacroProducer().fetch_and_write_to_snowflake(conn)
        logger.info('ingest_macro: wrote %d rows', n)
        _log_run(run_id, 'ingest_macro', 'completed',
                 row_count=n, started_at=started_at, completed_at=datetime.utcnow())
        return n
    except Exception as exc:
        _log_run(run_id, 'ingest_macro', 'failed',
                 error_msg=str(exc)[:500],
                 started_at=started_at, completed_at=datetime.utcnow())
        raise


def _ingest_fred(**ctx):
    """
    Task 1c: Fetch macro series from the FRED API → MERGE into RAW_FRED_INDICATORS.
    Runs in parallel with _ingest_prices and _ingest_macro.
    If FRED_API_KEY is empty, the producer returns 0 and the task succeeds.
    """
    import config as cfg
    from snowflake_client import get_connection
    from ingestion.fred_producer import FredProducer

    run_id     = ctx['run_id']
    started_at = datetime.utcnow()
    _log_run(run_id, 'ingest_fred', 'started', started_at=started_at)

    try:
        conn = get_connection()
        n    = FredProducer(cfg.FRED_API_KEY).fetch_and_write_to_snowflake(conn)
        logger.info('ingest_fred: wrote %d rows', n)
        _log_run(run_id, 'ingest_fred', 'completed',
                 row_count=n, started_at=started_at, completed_at=datetime.utcnow())
        return n
    except Exception as exc:
        _log_run(run_id, 'ingest_fred', 'failed',
                 error_msg=str(exc)[:500],
                 started_at=started_at, completed_at=datetime.utcnow())
        raise


def _ingest_sec_metadata(**ctx):
    """Task 1d: Discover recent SEC filings → MERGE into RAW_SEC_FILINGS."""
    import config as cfg
    from snowflake_client import get_connection
    from ingestion.sec_producer import SECProducer

    run_id     = ctx['run_id']
    started_at = datetime.utcnow()
    _log_run(run_id, 'ingest_sec_metadata', 'started', started_at=started_at)

    try:
        if not cfg.SEC_USER_AGENT:
            logger.warning('SEC_USER_AGENT not set — skipping SEC ingestion')
            _log_run(run_id, 'ingest_sec_metadata', 'completed',
                     row_count=0, started_at=started_at,
                     completed_at=datetime.utcnow())
            return 0
        conn = get_connection()
        n = SECProducer().fetch_filing_metadata(cfg.WATCHLIST_TICKERS, conn)
        logger.info('ingest_sec_metadata: wrote %d rows', n)
        _log_run(run_id, 'ingest_sec_metadata', 'completed',
                 row_count=n, started_at=started_at, completed_at=datetime.utcnow())
        return n
    except Exception as exc:
        _log_run(run_id, 'ingest_sec_metadata', 'failed',
                 error_msg=str(exc)[:500],
                 started_at=started_at, completed_at=datetime.utcnow())
        raise


def _ingest_sec_text(**ctx):
    """Task 1e: Fetch primary-doc HTML for new filings → RAW_SEC_FILING_TEXT."""
    import config as cfg
    from snowflake_client import get_connection
    from ingestion.sec_producer import SECProducer

    run_id     = ctx['run_id']
    started_at = datetime.utcnow()
    _log_run(run_id, 'ingest_sec_text', 'started', started_at=started_at)

    try:
        if not cfg.SEC_USER_AGENT:
            _log_run(run_id, 'ingest_sec_text', 'completed',
                     row_count=0, started_at=started_at,
                     completed_at=datetime.utcnow())
            return 0
        conn = get_connection()
        n = SECProducer().fetch_filing_text(conn)
        logger.info('ingest_sec_text: wrote %d chunks', n)
        _log_run(run_id, 'ingest_sec_text', 'completed',
                 row_count=n, started_at=started_at, completed_at=datetime.utcnow())
        return n
    except Exception as exc:
        _log_run(run_id, 'ingest_sec_text', 'failed',
                 error_msg=str(exc)[:500],
                 started_at=started_at, completed_at=datetime.utcnow())
        raise


def _summarize_sec_filings(**ctx):
    """Task 1f: Cortex-summarize filings with text → SEC_FILING_SUMMARIES."""
    from snowflake_client import get_connection
    from ingestion.sec_producer import SECProducer

    run_id     = ctx['run_id']
    started_at = datetime.utcnow()
    _log_run(run_id, 'summarize_sec_filings', 'started', started_at=started_at)

    try:
        conn = get_connection()
        n = SECProducer().summarize_filings(conn)
        logger.info('summarize_sec_filings: wrote %d rows', n)
        _log_run(run_id, 'summarize_sec_filings', 'completed',
                 row_count=n, started_at=started_at, completed_at=datetime.utcnow())
        return n
    except Exception as exc:
        _log_run(run_id, 'summarize_sec_filings', 'failed',
                 error_msg=str(exc)[:500],
                 started_at=started_at, completed_at=datetime.utcnow())
        raise


def _log_dbt_build_start(**ctx):
    """Record dbt build kickoff in PIPELINE_RUN_LOG so the dashboard sees it."""
    _log_run(ctx['run_id'], 'refresh_signals', 'started',
             started_at=datetime.utcnow())


def _log_dbt_build_end(**ctx):
    """Record dbt build completion. Runs only if the BashOperator succeeded."""
    _log_run(ctx['run_id'], 'refresh_signals', 'completed',
             started_at=datetime.utcnow(),
             completed_at=datetime.utcnow())


def _anomaly_check(**ctx):
    """
    Task 3: Query V_SIGNAL_SUMMARY for today's signals.
    Push results to XCom so the notify task can use them.
    """
    from snowflake_client import get_connection

    run_id     = ctx['run_id']
    started_at = datetime.utcnow()
    _log_run(run_id, 'anomaly_check', 'started', started_at=started_at)

    try:
        conn   = get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT DATE, SIGNAL_TYPE, ENTITY, MAGNITUDE, SALIENCE_SCORE, SUMMARY
            FROM SCORPION_DB.MARKETLENS.V_SIGNAL_SUMMARY
            WHERE DATE >= DATEADD(DAY, -1, CURRENT_DATE())
            ORDER BY ABS(SALIENCE_SCORE) DESC
            LIMIT 20
        """)
        rows = cursor.fetchall()
        cursor.close()

        logger.info('anomaly_check: found %d signals', len(rows))
        _log_run(run_id, 'anomaly_check', 'completed',
                 row_count=len(rows),
                 started_at=started_at, completed_at=datetime.utcnow())

        # Serialize for XCom (Airflow XCom stores JSON-serializable objects)
        return [list(r) for r in rows]
    except Exception as exc:
        _log_run(run_id, 'anomaly_check', 'failed',
                 error_msg=str(exc)[:500],
                 started_at=started_at, completed_at=datetime.utcnow())
        raise


def _notify(**ctx):
    """
    Task 4: Broadcast anomaly signals via all configured channels.
    Reads signal rows from XCom pushed by anomaly_check.
    Fail-open: a broken notification channel never fails the DAG.
    """
    import config as cfg
    from notification.base_sender import broadcast, build_signal_table
    from notification.slack_sender import SlackSender
    from notification.email_sender import EmailSender

    run_id     = ctx['run_id']
    started_at = datetime.utcnow()

    rows = ctx['ti'].xcom_pull(task_ids='anomaly_check') or []

    if not rows:
        logger.info('No signals to notify')
        _log_run(run_id, 'notify', 'completed',
                 started_at=started_at, completed_at=datetime.utcnow())
        return

    title = f'MarketLens Signals — {ctx["ds"]} ({len(rows)} signals)'
    body  = build_signal_table(rows)

    senders = [
        SlackSender(),
        EmailSender(),
    ]
    results = broadcast(senders, title, body)
    logger.info('Notification results: %s', results)
    _log_run(run_id, 'notify', 'completed',
             row_count=len(rows),
             started_at=started_at, completed_at=datetime.utcnow())


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

with DAG(
    dag_id='marketlens_daily',
    description='MarketLens daily ingestion, signal refresh, and anomaly alerts',
    schedule='0 23 * * 1-5',      # 23:00 UTC = ~18:00 ET on weekdays
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={
        'retries':         1,
        'retry_delay':     timedelta(minutes=5),
        'execution_timeout': timedelta(minutes=30),
    },
    tags=['marketlens'],
) as dag:

    ingest_prices = PythonOperator(
        task_id='ingest_prices',
        python_callable=_ingest_prices,
    )

    ingest_macro = PythonOperator(
        task_id='ingest_macro',
        python_callable=_ingest_macro,
    )

    ingest_fred = PythonOperator(
        task_id='ingest_fred',
        python_callable=_ingest_fred,
    )

    ingest_sec_metadata = PythonOperator(
        task_id='ingest_sec_metadata',
        python_callable=_ingest_sec_metadata,
        retries=2,
        retry_delay=timedelta(minutes=5),
    )

    ingest_sec_text = PythonOperator(
        task_id='ingest_sec_text',
        python_callable=_ingest_sec_text,
        retries=2,
        retry_delay=timedelta(minutes=5),
    )

    summarize_sec_filings = PythonOperator(
        task_id='summarize_sec_filings',
        python_callable=_summarize_sec_filings,
        retries=2,
        retry_delay=timedelta(minutes=5),
    )

    # Signal refresh is now a `dbt build` invocation. dbt builds every model
    # listed in dbt/models/ and runs all configured tests — any test failure
    # aborts the DAG so anomaly_check never queries a stale/broken view.
    log_dbt_start = PythonOperator(
        task_id='log_dbt_start',
        python_callable=_log_dbt_build_start,
    )

    refresh_signals = BashOperator(
        task_id='refresh_signals',
        bash_command=(
            'cd "$MARKETLENS_ROOT/dbt" && '
            'dbt deps --profiles-dir . && '
            'if [ "$SNOWFLAKE_PAID_DATA_AVAILABLE" = "true" ]; then '
            '  dbt build --profiles-dir . --select +signal_summary+; '
            'else '
            '  dbt build --profiles-dir . --select +signal_summary+ '
            '    --exclude stg_10y_treasury+ stg_unemployment+; '
            'fi'
        ),
        env={
            'MARKETLENS_ROOT': _ROOT_DIR,
            'SNOWFLAKE_PAID_DATA_AVAILABLE': os.environ.get('SNOWFLAKE_PAID_DATA_AVAILABLE', ''),
            'SNOWFLAKE_PRIVATE_KEY_PATH':    os.environ.get('SNOWFLAKE_PRIVATE_KEY_PATH', ''),
            'SNOWFLAKE_ACCOUNT':             os.environ.get('SNOWFLAKE_ACCOUNT', ''),
            'SNOWFLAKE_USER':                os.environ.get('SNOWFLAKE_USER', ''),
            'SNOWFLAKE_ROLE':                os.environ.get('SNOWFLAKE_ROLE', ''),
            'SNOWFLAKE_DATABASE':            os.environ.get('SNOWFLAKE_DATABASE', ''),
            'SNOWFLAKE_WAREHOUSE':           os.environ.get('SNOWFLAKE_WAREHOUSE', ''),
            'SNOWFLAKE_SCHEMA':              os.environ.get('SNOWFLAKE_SCHEMA', ''),
        },
        append_env=True,
    )

    log_dbt_end = PythonOperator(
        task_id='log_dbt_end',
        python_callable=_log_dbt_build_end,
    )

    anomaly_check = PythonOperator(
        task_id='anomaly_check',
        python_callable=_anomaly_check,
    )

    notify = PythonOperator(
        task_id='notify',
        python_callable=_notify,
    )

    # ingest_prices and ingest_macro run in parallel, then dbt refresh
    # (wrapped with PIPELINE_RUN_LOG markers), then anomaly check, then notify.
    ingest_sec_metadata >> ingest_sec_text >> summarize_sec_filings
    [ingest_prices, ingest_macro, ingest_fred, summarize_sec_filings] \
        >> log_dbt_start >> refresh_signals >> log_dbt_end \
        >> anomaly_check >> notify
