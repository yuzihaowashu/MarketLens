import os
import re
import sys
import logging

import streamlit as st
import snowflake.connector
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from config import (
    SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_DATABASE,
    SNOWFLAKE_SCHEMA, SNOWFLAKE_WAREHOUSE, SNOWFLAKE_WAREHOUSE_FALLBACKS,
    SNOWFLAKE_ROLE, SNOWFLAKE_PRIVATE_KEY_PATH,
)
from snowflake.connector.errors import ProgrammingError

logger = logging.getLogger(__name__)

_cached_connection = None


@st.cache_resource(show_spinner="Connecting to Snowflake...")
def _get_cached_connection():
    """Create and cache a single Snowflake connection for the lifetime of the app process."""
    key_path = SNOWFLAKE_PRIVATE_KEY_PATH
    if not os.path.isfile(key_path):
        raise FileNotFoundError(
            f"Snowflake RSA key not found at {key_path}. "
            "Set the SNOWFLAKE_PRIVATE_KEY env var or copy your key there."
        )
    conn = snowflake.connector.connect(
        account=SNOWFLAKE_ACCOUNT,
        user=SNOWFLAKE_USER,
        private_key=_load_private_key(key_path),
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        warehouse=SNOWFLAKE_WAREHOUSE,
        role=SNOWFLAKE_ROLE,
    )
    _activate_session(conn)
    return conn


_SAFE_IDENT = re.compile(r'^[A-Za-z_][A-Za-z0-9_$]*$')


def _sql_ident(name: str) -> str:
    """Format a Snowflake object name for USE … (unquoted when safe so case matches defaults)."""
    n = name.strip()
    if not n:
        raise ValueError('Empty Snowflake identifier')
    if _SAFE_IDENT.match(n):
        return n
    return '"' + n.replace('"', '""') + '"'


def _warehouse_try_order():
    """Primary warehouse first, then fallbacks (deduped by case-insensitive name)."""
    seen = set()
    out = []
    for w in [SNOWFLAKE_WAREHOUSE] + list(SNOWFLAKE_WAREHOUSE_FALLBACKS):
        name = (w or '').strip()
        if not name:
            continue
        key = name.upper()
        if key in seen:
            continue
        seen.add(key)
        out.append(name)
    return out


def _try_use_warehouse(cur):
    """
    Pick a warehouse the session can use. Primary name may not exist (002043); then try fallbacks.
    """
    order = _warehouse_try_order()
    if not order:
        raise ValueError(
            'No warehouse names configured. Set SNOWFLAKE_WAREHOUSE in .env.'
        )
    tried = []
    last_err = None
    primary = (SNOWFLAKE_WAREHOUSE or '').strip()
    for name in order:
        tried.append(name)
        try:
            cur.execute(f'USE WAREHOUSE {_sql_ident(name)}')
            if primary and name.upper() != primary.upper():
                logger.warning(
                    'Warehouse %r not available; using %r instead. '
                    'Update SNOWFLAKE_WAREHOUSE in .env to match your account.',
                    primary,
                    name,
                )
            return name
        except ProgrammingError as e:
            last_err = e
            msg = str(e).lower()
            transient = (
                'does not exist' in msg
                or '02000' in msg
                or '002043' in msg
                or 'cannot be performed' in msg
            )
            if transient:
                continue
            raise
    if last_err is None:
        raise RuntimeError(
            f'No usable warehouse after trying {tried}. Check SNOWFLAKE_WAREHOUSE / '
            'SNOWFLAKE_WAREHOUSE_FALLBACKS in .env.'
        )
    raise RuntimeError(
        f'No usable warehouse after trying {tried}. Set SNOWFLAKE_WAREHOUSE to a name from '
        'SHOW WAREHOUSES in Snowflake (your role must have USAGE), or extend '
        'SNOWFLAKE_WAREHOUSE_FALLBACKS in .env.'
    ) from last_err


def _activate_session(conn):
    """Ensure role, warehouse, database, and schema are active (fixes 57P03)."""
    if not (SNOWFLAKE_WAREHOUSE or '').strip() and not SNOWFLAKE_WAREHOUSE_FALLBACKS:
        raise ValueError(
            'SNOWFLAKE_WAREHOUSE is empty. Set it in .env or the environment.'
        )
    cur = conn.cursor()
    try:
        if SNOWFLAKE_ROLE:
            cur.execute(f'USE ROLE {_sql_ident(SNOWFLAKE_ROLE)}')
        _try_use_warehouse(cur)
        if SNOWFLAKE_DATABASE:
            cur.execute(f'USE DATABASE {_sql_ident(SNOWFLAKE_DATABASE)}')
        if SNOWFLAKE_SCHEMA:
            cur.execute(f'USE SCHEMA {_sql_ident(SNOWFLAKE_SCHEMA)}')
    finally:
        cur.close()


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

    # Prefer st.cache_resource connection (shared across all Streamlit sessions).
    # Fall back to the module-level global when called outside a Streamlit context
    # (e.g. migration scripts, DAG tasks).
    try:
        if not force_new:
            conn = _get_cached_connection()
            if not conn.is_closed():
                return conn
            # Cached connection is closed — clear the cache so it rebuilds.
            _get_cached_connection.clear()
        return _get_cached_connection()
    except Exception:
        # Outside Streamlit runtime: fall back to plain module-level cache.
        pass

    if not force_new and _cached_connection is not None:
        if not _cached_connection.is_closed():
            return _cached_connection
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
    _activate_session(_cached_connection)
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
