"""
Macro data producer for MarketLens.

Reads macro indicators from Snowflake marketplace views (free + paid)
and merges them into RAW_MACRO_INDICATORS for use by signal SQL views.

Sources:
  Free  — SNOWFLAKE_PUBLIC_DATA_FREE  (Fed Funds Rate, CPI)
  Paid  — SNOWFLAKE_PUBLIC_DATA_PAID  (10Y Treasury, BLS employment)
          Only attempted if SNOWFLAKE_PAID_DATA_AVAILABLE=true in .env
"""

from __future__ import annotations

import logging
import os
import sys
import uuid
from dataclasses import dataclass, field
from datetime import date
from typing import List, Optional

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

import config as cfg

logger = logging.getLogger(__name__)

# Whether the paid Snowflake marketplace subscription is available
_PAID_AVAILABLE = cfg.parse_env_bool('SNOWFLAKE_PAID_DATA_AVAILABLE', False)


@dataclass
class MacroIndicator:
    """Normalized macro data point — one variable, one geography, one date."""
    variable:   str          # 'FED_FUNDS_RATE', '10Y_TREASURY', 'UNEMPLOYMENT_RATE'
    geo_id:     str          # 'country/USA', state GEO_ID, etc.
    date:       date
    value:      Optional[float]
    source:     str          # 'snowflake_free', 'snowflake_paid'
    query_id:   str = field(default_factory=lambda: str(uuid.uuid4()))

    def to_dict(self) -> dict:
        return {
            'variable':  self.variable,
            'geo_id':    self.geo_id,
            'date':      self.date.isoformat(),
            'value':     self.value,
            'source':    self.source,
            'query_id':  self.query_id,
        }


class MacroProducer:
    """
    Fetches macro indicators from Snowflake marketplace and merges them
    into RAW_MACRO_INDICATORS.

    Unlike YFinanceProducer this is a Snowflake-to-Snowflake ETL —
    the data already lives in marketplace views; we just materialize
    a subset into our own schema for downstream signal SQL.
    """

    name = 'macro_snowflake'

    # ------------------------------------------------------------------
    # Free data fetchers (always available)
    # ------------------------------------------------------------------

    def _fetch_fed_funds_rate(self, cursor) -> List[MacroIndicator]:
        """Fed Funds Effective Rate from free marketplace."""
        query_id = str(uuid.uuid4())
        cursor.execute("""
            SELECT DATE, VALUE
            FROM SNOWFLAKE_PUBLIC_DATA_FREE.PUBLIC_DATA_FREE.FINANCIAL_ECONOMIC_INDICATORS_TIMESERIES
            WHERE VARIABLE = 'EFFR_PCT'
              AND GEO_ID   = 'country/USA'
              -- full history (was: AND DATE >= DATEADD(YEAR, -2, CURRENT_DATE()))
            ORDER BY DATE
        """)
        indicators = []
        for row in cursor.fetchall():
            indicators.append(MacroIndicator(
                variable='FED_FUNDS_RATE',
                geo_id='country/USA',
                date=row[0],
                value=float(row[1]) if row[1] is not None else None,
                source='snowflake_free',
                query_id=query_id,
            ))
        logger.info('Fetched %d Fed Funds Rate rows', len(indicators))
        return indicators

    def _fetch_cpi(self, cursor) -> List[MacroIndicator]:
        """CPI All Items from free marketplace."""
        query_id = str(uuid.uuid4())
        cursor.execute("""
            SELECT DATE, VALUE
            FROM SNOWFLAKE_PUBLIC_DATA_FREE.PUBLIC_DATA_FREE.BUREAU_OF_LABOR_STATISTICS_PRICE_TIMESERIES
            WHERE VARIABLE = 'CPI:_All_items,_Seasonally_adjusted,_Monthly'
              AND GEO_ID   = 'country/USA'
              -- full history (was: AND DATE >= DATEADD(YEAR, -2, CURRENT_DATE()))
            ORDER BY DATE
        """)
        indicators = []
        for row in cursor.fetchall():
            indicators.append(MacroIndicator(
                variable='CPI_ALL_ITEMS',
                geo_id='country/USA',
                date=row[0],
                value=float(row[1]) if row[1] is not None else None,
                source='snowflake_free',
                query_id=query_id,
            ))
        logger.info('Fetched %d CPI rows', len(indicators))
        return indicators

    # ------------------------------------------------------------------
    # Paid data fetchers (conditional on SNOWFLAKE_PAID_DATA_AVAILABLE)
    # ------------------------------------------------------------------

    def _fetch_10y_treasury(self, cursor) -> List[MacroIndicator]:
        """10-Year Treasury Yield from paid marketplace (NewSignals.sql)."""
        query_id = str(uuid.uuid4())
        cursor.execute("""
            SELECT DATE, VALUE
            FROM SNOWFLAKE_PUBLIC_DATA_PAID.PUBLIC_DATA.FEDERAL_RESERVE_TIMESERIES_PIT
            WHERE VARIABLE = 'Z1_FL073161113.Q'
              AND _EFFECTIVE_END_TIMESTAMP IS NULL
              -- full history (was: AND DATE >= DATEADD(YEAR, -2, CURRENT_DATE()))
            ORDER BY DATE
        """)
        indicators = []
        for row in cursor.fetchall():
            indicators.append(MacroIndicator(
                variable='10Y_TREASURY',
                geo_id='country/USA',
                date=row[0],
                value=float(row[1]) if row[1] is not None else None,
                source='snowflake_paid',
                query_id=query_id,
            ))
        logger.info('Fetched %d 10Y Treasury rows', len(indicators))
        return indicators

    def _fetch_unemployment(self, cursor) -> List[MacroIndicator]:
        """National unemployment rate from paid marketplace (NewSignals.sql)."""
        query_id = str(uuid.uuid4())
        cursor.execute("""
            SELECT DATE, VALUE
            FROM SNOWFLAKE_PUBLIC_DATA_PAID.PUBLIC_DATA.BUREAU_OF_LABOR_STATISTICS_EMPLOYMENT_TIMESERIES
            WHERE LOWER(SERIES_TITLE) LIKE '%unemployment rate%'
              AND LOWER(SERIES_TITLE) NOT LIKE '%not seasonally%'
              AND GEO_ID = 'country/USA'
              -- full history (was: AND DATE >= DATEADD(YEAR, -2, CURRENT_DATE()))
            ORDER BY DATE
        """)
        indicators = []
        for row in cursor.fetchall():
            indicators.append(MacroIndicator(
                variable='UNEMPLOYMENT_RATE',
                geo_id='country/USA',
                date=row[0],
                value=float(row[1]) if row[1] is not None else None,
                source='snowflake_paid',
                query_id=query_id,
            ))
        logger.info('Fetched %d unemployment rows', len(indicators))
        return indicators

    # ------------------------------------------------------------------
    # Aggregate all sources (fail-open: one source failing won't block others)
    # ------------------------------------------------------------------

    def fetch_all(self, conn) -> List[MacroIndicator]:
        """
        Collect macro indicators from all available sources.
        Each source is attempted independently — failure is logged but
        does not prevent other sources from running.
        """
        all_indicators: List[MacroIndicator] = []
        cursor = conn.cursor()

        free_fetchers = [
            ('Fed Funds Rate', self._fetch_fed_funds_rate),
            ('CPI',            self._fetch_cpi),
        ]
        for name, fn in free_fetchers:
            try:
                all_indicators.extend(fn(cursor))
            except Exception as exc:
                logger.warning('Free macro source [%s] failed: %s', name, exc)

        if _PAID_AVAILABLE:
            paid_fetchers = [
                ('10Y Treasury',  self._fetch_10y_treasury),
                ('Unemployment',  self._fetch_unemployment),
            ]
            for name, fn in paid_fetchers:
                try:
                    all_indicators.extend(fn(cursor))
                except Exception as exc:
                    logger.warning('Paid macro source [%s] failed: %s', name, exc)
        else:
            logger.info(
                'Skipping paid macro sources '
                '(set SNOWFLAKE_PAID_DATA_AVAILABLE=true to enable)'
            )

        cursor.close()
        return all_indicators

    # ------------------------------------------------------------------
    # Snowflake MERGE write
    # ------------------------------------------------------------------

    def fetch_and_write_to_snowflake(self, conn) -> int:
        """
        Fetch all macro indicators and bulk-load into RAW_MACRO_INDICATORS
        via a temporary stage table + single MERGE (fast path).
        Returns number of rows written.
        """
        import time
        import pandas as pd
        from snowflake.connector.pandas_tools import write_pandas

        indicators = self.fetch_all(conn)
        if not indicators:
            logger.warning('No macro indicators collected')
            return 0

        total = len(indicators)
        df = pd.DataFrame([{
            'VARIABLE': ind.variable,
            'GEO_ID':   ind.geo_id,
            'DATE':     ind.date,
            'VALUE':    ind.value,
            'SOURCE':   ind.source,
            'QUERY_ID': ind.query_id,
        } for ind in indicators])

        stage_table = f'TMP_MACRO_STAGE_{uuid.uuid4().hex[:8]}'.upper()
        cursor = conn.cursor()
        t0 = time.time()
        try:
            logger.info('MACRO: creating temp stage table %s', stage_table)
            cursor.execute(f"""
                CREATE OR REPLACE TEMPORARY TABLE SCORPION_DB.MARKETLENS.{stage_table} (
                    VARIABLE  VARCHAR(100),
                    GEO_ID    VARCHAR(100),
                    DATE      DATE,
                    VALUE     FLOAT,
                    SOURCE    VARCHAR(50),
                    QUERY_ID  VARCHAR(36)
                )
            """)

            logger.info('MACRO: bulk-loading %d rows via write_pandas', total)
            success, nchunks, nrows, _ = write_pandas(
                conn, df, stage_table,
                database='SCORPION_DB', schema='MARKETLENS',
                quote_identifiers=False,
            )
            logger.info('MACRO: write_pandas loaded %d rows in %d chunks (%.1fs)',
                        nrows, nchunks, time.time() - t0)

            t1 = time.time()
            cursor.execute(f"""
                MERGE INTO SCORPION_DB.MARKETLENS.RAW_MACRO_INDICATORS AS tgt
                USING SCORPION_DB.MARKETLENS.{stage_table} AS src
                  ON (tgt.VARIABLE = src.VARIABLE
                  AND tgt.GEO_ID   = src.GEO_ID
                  AND tgt.DATE     = src.DATE)
                WHEN MATCHED THEN UPDATE SET
                    VALUE       = src.VALUE,
                    SOURCE      = src.SOURCE,
                    QUERY_ID    = src.QUERY_ID,
                    INGESTED_AT = CURRENT_TIMESTAMP()
                WHEN NOT MATCHED THEN INSERT
                    (VARIABLE, GEO_ID, DATE, VALUE, SOURCE, QUERY_ID)
                VALUES
                    (src.VARIABLE, src.GEO_ID, src.DATE,
                     src.VALUE, src.SOURCE, src.QUERY_ID)
            """)
            logger.info('MACRO: MERGE completed in %.1fs (total %.1fs for %d rows)',
                        time.time() - t1, time.time() - t0, total)
            return total
        finally:
            cursor.close()
