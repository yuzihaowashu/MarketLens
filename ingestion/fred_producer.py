"""
FRED API producer for MarketLens.

Fetches economic time series from the FRED API (St. Louis Fed) and merges
them into RAW_FRED_INDICATORS for use by downstream signal SQL views.

Series fetched:
  GDPC1   → GDP_REAL                  (Real GDP, quarterly)
  HOUST   → HOUSING_STARTS            (Housing starts, monthly SAAR)
  UMCSENT → CONSUMER_SENTIMENT        (UMich consumer sentiment, monthly)
  T10YIE  → INFLATION_EXPECTATIONS_10Y (10Y breakeven inflation, daily)
  DGS10   → TREASURY_10Y              (10Y Treasury constant maturity yield, daily)

Requires FRED_API_KEY in config (free: https://fred.stlouisfed.org/docs/api/api_key.html).
"""

from __future__ import annotations

import logging
import os
import sys
import time
import uuid
from datetime import date, datetime
from typing import List, Optional

import requests

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

import config as cfg
from ingestion.macro_producer import MacroIndicator

logger = logging.getLogger(__name__)

FRED_BASE_URL = 'https://api.stlouisfed.org/fred/series/observations'


class FredProducer:
    """
    Fetches macro indicators from the FRED API and merges them into
    RAW_FRED_INDICATORS. Fail-open per series.
    """

    name = 'fred'

    def __init__(self, api_key: Optional[str] = None, timeout: int = 30,
                 start_date: Optional[str] = None):
        self.api_key = api_key or cfg.FRED_API_KEY
        self.timeout = timeout
        self.session = requests.Session()
        self.start_date = start_date or self.DEFAULT_START_DATE

    # ------------------------------------------------------------------
    # Single-series fetch
    # ------------------------------------------------------------------

    def _fetch_series(self, series_id: str, variable_name: str,
                      start_date: Optional[str] = None) -> List[MacroIndicator]:
        """
        GET /fred/series/observations for a single series_id. Parses
        observations, skips FRED's '.' missing-value marker, and returns
        MacroIndicator rows tagged with variable_name + source='fred'.
        """
        if not self.api_key:
            raise RuntimeError('FRED_API_KEY is not set')

        params = {
            'series_id':        series_id,
            'api_key':          self.api_key,
            'file_type':        'json',
        }
        if start_date:
            params['observation_start'] = start_date

        t0 = time.time()
        logger.info('FRED: GET %s  start=%s', series_id, start_date or '(none)')
        resp = self.session.get(FRED_BASE_URL, params=params, timeout=self.timeout)
        resp.raise_for_status()
        payload = resp.json()
        logger.info('FRED: %s HTTP %s in %.2fs', series_id, resp.status_code, time.time() - t0)

        query_id = str(uuid.uuid4())
        indicators: List[MacroIndicator] = []
        for obs in payload.get('observations', []):
            raw_val = obs.get('value')
            if raw_val is None or raw_val == '.':
                continue
            try:
                value = float(raw_val)
            except (TypeError, ValueError):
                continue
            try:
                obs_date = datetime.strptime(obs['date'], '%Y-%m-%d').date()
            except (KeyError, ValueError):
                continue
            indicators.append(MacroIndicator(
                variable=variable_name,
                geo_id='country/USA',
                date=obs_date,
                value=value,
                source='fred',
                query_id=query_id,
            ))

        logger.info('FRED: fetched %d rows for %s (%s)',
                    len(indicators), variable_name, series_id)
        return indicators

    # ------------------------------------------------------------------
    # Series wrappers
    # ------------------------------------------------------------------

    # None = pull full series history from FRED.
    DEFAULT_START_DATE: Optional[str] = None

    def _fetch_gdp(self) -> List[MacroIndicator]:
        return self._fetch_series(cfg.FRED_SERIES_GDP, 'GDP_REAL',
                                  start_date=self.start_date)

    def _fetch_housing_starts(self) -> List[MacroIndicator]:
        return self._fetch_series(cfg.FRED_SERIES_HOUSING_STARTS, 'HOUSING_STARTS',
                                  start_date=self.start_date)

    def _fetch_consumer_sentiment(self) -> List[MacroIndicator]:
        return self._fetch_series(cfg.FRED_SERIES_CONSUMER_SENTIMENT, 'CONSUMER_SENTIMENT',
                                  start_date=self.start_date)

    def _fetch_inflation_expectations(self) -> List[MacroIndicator]:
        return self._fetch_series(cfg.FRED_SERIES_INFLATION_EXPECTATIONS,
                                  'INFLATION_EXPECTATIONS_10Y',
                                  start_date=self.start_date)

    def _fetch_treasury_10y(self) -> List[MacroIndicator]:
        return self._fetch_series(cfg.FRED_SERIES_TREASURY_10Y,
                                  'TREASURY_10Y',
                                  start_date=self.start_date)

    # ------------------------------------------------------------------
    # Aggregate (fail-open per series)
    # ------------------------------------------------------------------

    def fetch_all(self) -> List[MacroIndicator]:
        all_indicators: List[MacroIndicator] = []
        fetchers = [
            ('GDP',                    self._fetch_gdp),
            ('Housing Starts',         self._fetch_housing_starts),
            ('Consumer Sentiment',     self._fetch_consumer_sentiment),
            ('Inflation Expectations', self._fetch_inflation_expectations),
            ('10Y Treasury Yield',     self._fetch_treasury_10y),
        ]
        for name, fn in fetchers:
            try:
                all_indicators.extend(fn())
            except Exception as exc:
                logger.warning('FRED source [%s] failed: %s', name, exc)
        return all_indicators

    # ------------------------------------------------------------------
    # Snowflake MERGE write
    # ------------------------------------------------------------------

    def fetch_and_write_to_snowflake(self, conn) -> int:
        """
        Fetch all FRED series and MERGE into RAW_FRED_INDICATORS.
        Returns number of rows written. If FRED_API_KEY is empty, logs
        and returns 0 (fail-open, consistent with paid-data gating).
        """
        if not self.api_key:
            logger.warning('FRED_API_KEY not set — skipping FRED ingestion')
            return 0

        indicators = self.fetch_all()
        if not indicators:
            logger.warning('No FRED indicators collected')
            return 0

        import pandas as pd
        from snowflake.connector.pandas_tools import write_pandas

        series_id_by_variable = {
            'GDP_REAL':                   cfg.FRED_SERIES_GDP,
            'HOUSING_STARTS':             cfg.FRED_SERIES_HOUSING_STARTS,
            'CONSUMER_SENTIMENT':         cfg.FRED_SERIES_CONSUMER_SENTIMENT,
            'INFLATION_EXPECTATIONS_10Y': cfg.FRED_SERIES_INFLATION_EXPECTATIONS,
            'TREASURY_10Y':               cfg.FRED_SERIES_TREASURY_10Y,
        }

        total = len(indicators)
        df = pd.DataFrame([{
            'VARIABLE':  ind.variable,
            'SERIES_ID': series_id_by_variable.get(ind.variable, ''),
            'GEO_ID':    ind.geo_id,
            'DATE':      ind.date,
            'VALUE':     ind.value,
            'SOURCE':    ind.source,
            'QUERY_ID':  ind.query_id,
        } for ind in indicators])

        stage_table = f'TMP_FRED_STAGE_{uuid.uuid4().hex[:8]}'.upper()
        cursor = conn.cursor()
        t0 = time.time()
        try:
            logger.info('FRED: creating temp stage table %s', stage_table)
            cursor.execute(f"""
                CREATE OR REPLACE TEMPORARY TABLE SCORPION_DB.MARKETLENS.{stage_table} (
                    VARIABLE   VARCHAR(100),
                    SERIES_ID  VARCHAR(50),
                    GEO_ID     VARCHAR(100),
                    DATE       DATE,
                    VALUE      FLOAT,
                    SOURCE     VARCHAR(50),
                    QUERY_ID   VARCHAR(36)
                )
            """)

            logger.info('FRED: bulk-loading %d rows via write_pandas', total)
            success, nchunks, nrows, _ = write_pandas(
                conn, df, stage_table,
                database='SCORPION_DB', schema='MARKETLENS',
                quote_identifiers=False,
            )
            logger.info('FRED: write_pandas loaded %d rows in %d chunks (%.1fs)',
                        nrows, nchunks, time.time() - t0)

            t1 = time.time()
            logger.info('FRED: executing single MERGE from stage table')
            cursor.execute(f"""
                MERGE INTO SCORPION_DB.MARKETLENS.RAW_FRED_INDICATORS AS tgt
                USING SCORPION_DB.MARKETLENS.{stage_table} AS src
                  ON (tgt.VARIABLE = src.VARIABLE
                  AND tgt.GEO_ID   = src.GEO_ID
                  AND tgt.DATE     = src.DATE)
                WHEN MATCHED THEN UPDATE SET
                    SERIES_ID   = src.SERIES_ID,
                    VALUE       = src.VALUE,
                    SOURCE      = src.SOURCE,
                    QUERY_ID    = src.QUERY_ID,
                    INGESTED_AT = CURRENT_TIMESTAMP()
                WHEN NOT MATCHED THEN INSERT
                    (VARIABLE, SERIES_ID, GEO_ID, DATE, VALUE, SOURCE, QUERY_ID)
                VALUES
                    (src.VARIABLE, src.SERIES_ID, src.GEO_ID, src.DATE,
                     src.VALUE, src.SOURCE, src.QUERY_ID)
            """)
            logger.info('FRED: MERGE completed in %.1fs (total %.1fs for %d rows)',
                        time.time() - t1, time.time() - t0, total)
            return total
        finally:
            cursor.close()
