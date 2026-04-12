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


# ---------------------------------------------------------------------------
# Task callables
# ---------------------------------------------------------------------------

def _log_run(run_id: str, task_id: str, status: str,
             row_count: int = 0, error_msg: str = '',
             started_at: datetime = None, completed_at: datetime = None):
    """Write a row to PIPELINE_RUN_LOG (best-effort; failure is non-fatal)."""
    try:
        import config as cfg
        if not cfg.PIPELINE_LOG_ENABLED:
            return
        from snowflake_client import get_connection
        conn   = get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            MERGE INTO SCORPION_DB.MARKETLENS.PIPELINE_RUN_LOG AS tgt
            USING (
                SELECT
                    %(run_id)s       AS RUN_ID,
                    'marketlens_daily' AS DAG_ID,
                    %(task_id)s      AS TASK_ID,
                    %(status)s       AS STATUS,
                    %(row_count)s    AS ROW_COUNT,
                    %(error_msg)s    AS ERROR_MSG,
                    %(started_at)s   AS STARTED_AT,
                    %(completed_at)s AS COMPLETED_AT
            ) AS src ON (tgt.RUN_ID = src.RUN_ID AND tgt.TASK_ID = src.TASK_ID)
            WHEN MATCHED THEN UPDATE SET
                STATUS       = src.STATUS,
                ROW_COUNT    = src.ROW_COUNT,
                ERROR_MSG    = src.ERROR_MSG,
                COMPLETED_AT = src.COMPLETED_AT
            WHEN NOT MATCHED THEN INSERT
                (RUN_ID, DAG_ID, TASK_ID, STATUS, ROW_COUNT,
                 ERROR_MSG, STARTED_AT, COMPLETED_AT)
            VALUES
                (src.RUN_ID, src.DAG_ID, src.TASK_ID, src.STATUS,
                 src.ROW_COUNT, src.ERROR_MSG, src.STARTED_AT, src.COMPLETED_AT)
        """, {
            'run_id':       run_id,
            'task_id':      task_id,
            'status':       status,
            'row_count':    row_count,
            'error_msg':    error_msg or '',
            'started_at':   started_at or datetime.utcnow(),
            'completed_at': completed_at,
        })
        cursor.close()
    except Exception as exc:
        logger.warning('PIPELINE_RUN_LOG write failed (non-fatal): %s', exc)


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


def _refresh_signals(**ctx):
    """
    Task 2: Re-execute the signal SQL view chain.

    The views are defined against the base views in setup.sql which read
    from the free marketplace.  This task ensures any new rows in
    RAW_STOCK_PRICES are visible to downstream queries by running a
    lightweight SELECT that materialises the view plan.

    NOTE: Snowflake views are not materialized; this task queries each
    view to force a plan refresh and validate that no view is broken.
    A SnowflakeOperator could also be used here with the provider package.
    """
    from snowflake_client import get_connection

    run_id     = ctx['run_id']
    started_at = datetime.utcnow()
    _log_run(run_id, 'refresh_signals', 'started', started_at=started_at)

    views = [
        'V_STOCK_PRICES',
        'V_DAILY_RETURNS',
        'V_ROLLING_VOLATILITY',
        'V_ANOMALY_SCORES',
        'V_FED_RATE_CHANGES',
        'V_CPI_CHANGES',
        'V_SIGNAL_SUMMARY',
    ]
    try:
        conn   = get_connection()
        cursor = conn.cursor()
        for view in views:
            cursor.execute(
                f'SELECT COUNT(*) FROM SCORPION_DB.MARKETLENS.{view}'
            )
            count = cursor.fetchone()[0]
            logger.info('View %s has %d rows', view, count)
        cursor.close()
        _log_run(run_id, 'refresh_signals', 'completed',
                 started_at=started_at, completed_at=datetime.utcnow())
    except Exception as exc:
        _log_run(run_id, 'refresh_signals', 'failed',
                 error_msg=str(exc)[:500],
                 started_at=started_at, completed_at=datetime.utcnow())
        raise


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

    refresh_signals = PythonOperator(
        task_id='refresh_signals',
        python_callable=_refresh_signals,
    )

    anomaly_check = PythonOperator(
        task_id='anomaly_check',
        python_callable=_anomaly_check,
    )

    notify = PythonOperator(
        task_id='notify',
        python_callable=_notify,
    )

    # ingest_prices and ingest_macro run in parallel, then signal refresh,
    # then anomaly check, then notifications
    [ingest_prices, ingest_macro] >> refresh_signals >> anomaly_check >> notify
