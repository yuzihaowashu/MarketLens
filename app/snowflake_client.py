import os
import sys
import logging

import snowflake.connector
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from config import (
    SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_DATABASE,
    SNOWFLAKE_SCHEMA, SNOWFLAKE_WAREHOUSE, SNOWFLAKE_ROLE,
    SNOWFLAKE_PRIVATE_KEY_PATH,
)

logger = logging.getLogger(__name__)

_cached_connection = None


def _load_private_key(path: str):
    with open(path, 'rb') as f:
        private_key = serialization.load_pem_private_key(
            f.read(), password=None, backend=default_backend()
        )
    return private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


def get_connection(force_new=False):
    """Return a reusable Snowflake connection, reconnecting when stale."""
    global _cached_connection

    if not force_new and _cached_connection is not None:
        try:
            cur = _cached_connection.cursor()
            cur.execute("SELECT 1")
            cur.close()
            return _cached_connection
        except Exception:
            logger.debug("Cached connection stale, reconnecting")
            try:
                _cached_connection.close()
            except Exception:
                pass
            _cached_connection = None

    key_path = SNOWFLAKE_PRIVATE_KEY_PATH
    if not os.path.isfile(key_path):
        raise FileNotFoundError(
            f"Snowflake RSA key not found at {key_path}. "
            "Set the SNOWFLAKE_PRIVATE_KEY env var or copy your key there."
        )

    _cached_connection = snowflake.connector.connect(
        account=SNOWFLAKE_ACCOUNT,
        user=SNOWFLAKE_USER,
        private_key=_load_private_key(key_path),
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        warehouse=SNOWFLAKE_WAREHOUSE,
        role=SNOWFLAKE_ROLE,
    )
    return _cached_connection


def run_query(sql: str, params=None):
    """Execute *sql* and return ``(column_names, rows)``."""
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(sql, params)
        columns = [c[0] for c in cursor.description]
        rows = cursor.fetchall()
        return columns, rows
    except Exception:
        logger.exception("Query failed: %s", sql[:200])
        raise
    finally:
        cursor.close()


def run_query_single(sql: str, params=None):
    """Execute *sql* and return the first column of the first row, or ``None``."""
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(sql, params)
        row = cursor.fetchone()
        return row[0] if row else None
    except Exception:
        logger.exception("Query failed: %s", sql[:200])
        raise
    finally:
        cursor.close()
