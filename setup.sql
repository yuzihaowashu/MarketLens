-- MarketLens: Schema and base views setup
-- Run this once to bootstrap the MARKETLENS schema in SCORPION_DB.

USE ROLE TRAINING_ROLE;
USE WAREHOUSE SCORPION_WH;
USE DATABASE SCORPION_DB;

CREATE SCHEMA IF NOT EXISTS MARKETLENS;
USE SCHEMA MARKETLENS;

----------------------------------------------------------------------
-- Base view: stock prices (close + volume) for watchlist tickers
----------------------------------------------------------------------
CREATE OR REPLACE VIEW V_STOCK_PRICES AS
SELECT
    TICKER,
    DATE,
    MAX(CASE WHEN VARIABLE = 'post-market_close_adjusted' THEN VALUE END) AS CLOSE_PRICE,
    MAX(CASE WHEN VARIABLE = 'pre-market_open_adjusted'   THEN VALUE END) AS OPEN_PRICE,
    MAX(CASE WHEN VARIABLE = 'all-day_high_adjusted'       THEN VALUE END) AS HIGH_PRICE,
    MAX(CASE WHEN VARIABLE = 'all-day_low_adjusted'        THEN VALUE END) AS LOW_PRICE,
    MAX(CASE WHEN VARIABLE = 'nasdaq_volume'               THEN VALUE END) AS VOLUME
FROM SNOWFLAKE_PUBLIC_DATA_FREE.PUBLIC_DATA_FREE.STOCK_PRICE_TIMESERIES
WHERE TICKER IN ('AAPL','MSFT','GOOGL','AMZN','TSLA','NVDA','META','SPY','QQQ')
  AND VARIABLE IN (
      'post-market_close_adjusted','pre-market_open_adjusted',
      'all-day_high_adjusted','all-day_low_adjusted','nasdaq_volume'
  )
GROUP BY TICKER, DATE;

----------------------------------------------------------------------
-- Base view: Federal Funds Rate (daily)
----------------------------------------------------------------------
CREATE OR REPLACE VIEW V_FED_FUNDS_RATE AS
SELECT
    DATE,
    VALUE AS FED_FUNDS_RATE
FROM SNOWFLAKE_PUBLIC_DATA_FREE.PUBLIC_DATA_FREE.FINANCIAL_ECONOMIC_INDICATORS_TIMESERIES
WHERE VARIABLE = 'EFFR_PCT'
  AND GEO_ID = 'country/USA'
ORDER BY DATE;

----------------------------------------------------------------------
-- Base view: CPI All Items (monthly)
----------------------------------------------------------------------
CREATE OR REPLACE VIEW V_CPI AS
SELECT
    DATE,
    VALUE AS CPI_INDEX
FROM SNOWFLAKE_PUBLIC_DATA_FREE.PUBLIC_DATA_FREE.BUREAU_OF_LABOR_STATISTICS_PRICE_TIMESERIES
WHERE VARIABLE = 'CPI:_All_items,_Seasonally_adjusted,_Monthly'
  AND GEO_ID = 'country/USA'
ORDER BY DATE;

----------------------------------------------------------------------
-- Base view: SEC corporate report text (for RAG)
----------------------------------------------------------------------
CREATE OR REPLACE VIEW V_SEC_FILINGS AS
SELECT
    SEC_DOCUMENT_ID,
    ADSH,
    FORM_TYPE,
    VARIABLE_NAME,
    VALUE AS FILING_TEXT
FROM SNOWFLAKE_PUBLIC_DATA_FREE.PUBLIC_DATA_FREE.SEC_CORPORATE_REPORT_TEXT_ATTRIBUTES
WHERE FORM_TYPE IN ('10-K', '10-Q', '8-K', '10-K_A', '10-Q_A')
  AND LENGTH(VALUE) > 200;
