-- MarketLens: Staging tables for ingestion layer
-- Run once via: ./start.sh --setup  (add this file to the SQL_FILES list in start.sh)
-- or execute manually in Snowsight.

USE ROLE TRAINING_ROLE;
USE WAREHOUSE SCORPION_WH;
USE DATABASE SCORPION_DB;
USE SCHEMA MARKETLENS;

-- ---------------------------------------------------------------------------
-- RAW_STOCK_PRICES
-- Written by YFinanceProducer (Phase 1: direct) or SnowflakePricesConsumer
-- (Phase 2: via Kafka).  PRIMARY KEY enforces idempotent MERGE.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS RAW_STOCK_PRICES (
    TICKER       VARCHAR(20)   NOT NULL,
    DATE         DATE          NOT NULL,
    OPEN_PRICE   FLOAT,
    HIGH_PRICE   FLOAT,
    LOW_PRICE    FLOAT,
    CLOSE_PRICE  FLOAT,
    VOLUME       BIGINT,
    SOURCE       VARCHAR(50)   NOT NULL DEFAULT 'yfinance',
    QUERY_ID     VARCHAR(36),           -- UUID for lineage tracing
    INGESTED_AT  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (TICKER, DATE, SOURCE)
);

-- ---------------------------------------------------------------------------
-- RAW_MACRO_INDICATORS
-- Written by MacroProducer from Snowflake marketplace (free + paid).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS RAW_MACRO_INDICATORS (
    VARIABLE     VARCHAR(100)  NOT NULL,   -- 'FED_FUNDS_RATE', '10Y_TREASURY', etc.
    GEO_ID       VARCHAR(100)  NOT NULL,   -- 'country/USA', state GEO_ID, etc.
    DATE         DATE          NOT NULL,
    VALUE        FLOAT,
    SOURCE       VARCHAR(50)   NOT NULL DEFAULT 'snowflake_free',
    QUERY_ID     VARCHAR(36),
    INGESTED_AT  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (VARIABLE, GEO_ID, DATE)
);

-- ---------------------------------------------------------------------------
-- PIPELINE_RUN_LOG
-- Written by the Airflow DAG at start and end of each run.
-- Used by the Pipeline Health dashboard page.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS PIPELINE_RUN_LOG (
    RUN_ID       VARCHAR(36)   NOT NULL,
    DAG_ID       VARCHAR(100),
    TASK_ID      VARCHAR(100),
    STATUS       VARCHAR(20),            -- 'started', 'completed', 'failed'
    TICKERS      VARIANT,               -- JSON array of processed tickers
    ROW_COUNT    INTEGER,               -- rows ingested in this task
    ERROR_MSG    VARCHAR(2000),
    STARTED_AT   TIMESTAMP_NTZ,
    COMPLETED_AT TIMESTAMP_NTZ,
    PRIMARY KEY (RUN_ID, TASK_ID)
);
