-- MarketLens: FRED API staging table
-- Run once via: ./start.sh --setup  (included in SQL_FILES list in start.sh)
-- or execute manually in Snowsight.

USE ROLE TRAINING_ROLE;
USE WAREHOUSE SCORPION_WH;
USE DATABASE SCORPION_DB;
USE SCHEMA MARKETLENS;

-- ---------------------------------------------------------------------------
-- RAW_FRED_INDICATORS
-- Written by FredProducer from the FRED API (St. Louis Fed).
-- Kept separate from RAW_MACRO_INDICATORS (which is sourced from the
-- Snowflake marketplace) so the two producers can evolve independently.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS RAW_FRED_INDICATORS (
    VARIABLE     VARCHAR(100)  NOT NULL,   -- 'GDP_REAL', 'HOUSING_STARTS', etc.
    SERIES_ID    VARCHAR(50)   NOT NULL,   -- FRED series id, e.g. 'GDPC1'
    GEO_ID       VARCHAR(100)  NOT NULL DEFAULT 'country/USA',
    DATE         DATE          NOT NULL,
    VALUE        FLOAT,
    SOURCE       VARCHAR(50)   NOT NULL DEFAULT 'fred',
    QUERY_ID     VARCHAR(36),
    INGESTED_AT  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (VARIABLE, GEO_ID, DATE)
);
