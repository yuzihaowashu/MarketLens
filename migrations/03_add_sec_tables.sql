-- MarketLens: SEC EDGAR ingestion tables.
-- Run once via Snowsight or ./start.sh --setup.

USE ROLE TRAINING_ROLE;
USE WAREHOUSE SCORPION_WH;
USE DATABASE SCORPION_DB;
USE SCHEMA MARKETLENS;

-- ---------------------------------------------------------------------------
-- RAW_SEC_FILINGS
-- One row per filing discovered via data.sec.gov/submissions/CIK##########.json.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS RAW_SEC_FILINGS (
    CIK                VARCHAR(10)   NOT NULL,
    TICKER             VARCHAR(20),
    COMPANY_NAME       VARCHAR(500),
    ACCESSION_NUMBER   VARCHAR(25)   NOT NULL,
    FORM_TYPE          VARCHAR(20)   NOT NULL,
    FILING_DATE        DATE,
    REPORT_DATE        DATE,
    PRIMARY_DOC_URL    VARCHAR(1000),
    TEXT_INGESTED_AT   TIMESTAMP_NTZ,
    QUERY_ID           VARCHAR(36),
    INGESTED_AT        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (CIK, ACCESSION_NUMBER)
);

-- ---------------------------------------------------------------------------
-- RAW_SEC_FILING_TEXT
-- Cleaned + chunked filing text. One row per (filing, section, chunk).
-- SECTION ∈ {risk, mdna, business, other}.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS RAW_SEC_FILING_TEXT (
    ACCESSION_NUMBER   VARCHAR(25)   NOT NULL,
    SECTION            VARCHAR(20)   NOT NULL,
    CHUNK_IX           INTEGER       NOT NULL,
    CONTENT            VARCHAR(16777216),
    CHAR_COUNT         INTEGER,
    INGESTED_AT        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (ACCESSION_NUMBER, SECTION, CHUNK_IX)
);

-- ---------------------------------------------------------------------------
-- SEC_FILING_SUMMARIES
-- One row per filing with Cortex-generated narratives.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS SEC_FILING_SUMMARIES (
    ACCESSION_NUMBER           VARCHAR(25)   NOT NULL,
    TICKER                     VARCHAR(20),
    FORM_TYPE                  VARCHAR(20),
    FILING_DATE                DATE,
    REVENUE_NARRATIVE          VARCHAR(16777216),
    GUIDANCE_NARRATIVE         VARCHAR(16777216),
    RISK_NARRATIVE             VARCHAR(16777216),
    MANAGEMENT_TONE            VARCHAR(32),
    EARNINGS_CONTEXT_SUMMARY   VARCHAR(16777216),
    MODEL                      VARCHAR(100),
    SUMMARIZED_AT              TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (ACCESSION_NUMBER)
);

-- ---------------------------------------------------------------------------
-- SEC_INGEST_ERRORS
-- Per-filing granular errors that shouldn't fail the whole DAG task.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS SEC_INGEST_ERRORS (
    ACCESSION_NUMBER   VARCHAR(25),
    STAGE              VARCHAR(20),    -- 'meta' | 'text' | 'summary'
    ERROR_MSG          VARCHAR(2000),
    HTTP_STATUS        INTEGER,
    RAW_SNIPPET        VARCHAR(1000),
    OCCURRED_AT        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
