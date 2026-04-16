"""
Shared helper for writing to PIPELINE_RUN_LOG.

Used by both the Airflow DAG (dags/marketlens_daily.py) and the standalone
ingest runner (run_fred_ingest.py) so the Pipeline Health dashboard always
sees runs regardless of how the pipeline was triggered.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)


def log_run(
    run_id: str,
    task_id: str,
    status: str,
    dag_id: str = 'marketlens_daily',
    row_count: int = 0,
    error_msg: str = '',
    started_at: Optional[datetime] = None,
    completed_at: Optional[datetime] = None,
) -> None:
    """Write a row to PIPELINE_RUN_LOG (best-effort; failure is non-fatal)."""
    try:
        import config as cfg
        if not cfg.PIPELINE_LOG_ENABLED:
            return
        from snowflake_client import get_connection
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            MERGE INTO SCORPION_DB.MARKETLENS.PIPELINE_RUN_LOG AS tgt
            USING (
                SELECT
                    %(run_id)s       AS RUN_ID,
                    %(dag_id)s       AS DAG_ID,
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
            'dag_id':       dag_id,
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
