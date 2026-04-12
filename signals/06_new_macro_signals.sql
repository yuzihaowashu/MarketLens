-- MarketLens: Extended macro signal views
-- Formalizes NewSignals.sql (from new-signals-branch) into proper schema views.
--
-- NOTE: V_10Y_TREASURY and V_UNEMPLOYMENT_RATE require the
--       SNOWFLAKE_PUBLIC_DATA_PAID marketplace subscription.
--       V_YIELD_CURVE joins those two views.
--       If the paid dataset is unavailable, skip this file —
--       the rest of the signal pipeline will still work.

USE SCHEMA SCORPION_DB.MARKETLENS;

-- ---------------------------------------------------------------------------
-- 10-Year Treasury Yield (quarterly, last 10 years)
-- Source: Federal Reserve Z.1 Financial Accounts (paid marketplace)
-- Variable: Z1_FL073161113.Q
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW V_10Y_TREASURY AS
SELECT
    DATE,
    VALUE AS YIELD_PCT
FROM SNOWFLAKE_PUBLIC_DATA_PAID.PUBLIC_DATA.FEDERAL_RESERVE_TIMESERIES_PIT
WHERE VARIABLE = 'Z1_FL073161113.Q'
  AND _EFFECTIVE_END_TIMESTAMP IS NULL
  AND DATE >= DATEADD(YEAR, -10, CURRENT_DATE())
ORDER BY DATE;

-- ---------------------------------------------------------------------------
-- National Unemployment Rate (monthly)
-- Source: Bureau of Labor Statistics Employment (paid marketplace)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW V_UNEMPLOYMENT_RATE AS
SELECT
    DATE,
    VALUE          AS UNEMPLOYMENT_PCT,
    GEO_ID,
    SERIES_TITLE
FROM SNOWFLAKE_PUBLIC_DATA_PAID.PUBLIC_DATA.BUREAU_OF_LABOR_STATISTICS_EMPLOYMENT_TIMESERIES
WHERE LOWER(SERIES_TITLE) LIKE '%unemployment rate%'
  AND LOWER(SERIES_TITLE) NOT LIKE '%not seasonally%'
  AND GEO_ID = 'country/USA'
ORDER BY DATE;

-- ---------------------------------------------------------------------------
-- Yield Curve: 10Y Treasury minus Fed Funds Rate
-- Negative spread = inverted yield curve (recession signal)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW V_YIELD_CURVE AS
SELECT
    t.DATE,
    t.YIELD_PCT                                   AS TREASURY_10Y,
    f.FED_FUNDS_RATE,
    t.YIELD_PCT - f.FED_FUNDS_RATE                AS CURVE_SPREAD,
    CASE
        WHEN t.YIELD_PCT - f.FED_FUNDS_RATE < 0
        THEN TRUE ELSE FALSE
    END                                            AS IS_INVERTED
FROM V_10Y_TREASURY t
JOIN V_FED_FUNDS_RATE f
  ON t.DATE = f.DATE
ORDER BY t.DATE;

-- ---------------------------------------------------------------------------
-- Yield curve change signal (for V_SIGNAL_SUMMARY integration)
-- Flags quarters where the inversion status flips
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW V_YIELD_CURVE_SIGNALS AS
SELECT
    DATE,
    CURVE_SPREAD,
    IS_INVERTED,
    LAG(IS_INVERTED) OVER (ORDER BY DATE) AS PREV_INVERTED,
    CASE
        WHEN IS_INVERTED = TRUE  AND LAG(IS_INVERTED) OVER (ORDER BY DATE) = FALSE
        THEN 'INVERSION_START'
        WHEN IS_INVERTED = FALSE AND LAG(IS_INVERTED) OVER (ORDER BY DATE) = TRUE
        THEN 'INVERSION_END'
        ELSE NULL
    END AS FLIP_EVENT
FROM V_YIELD_CURVE
QUALIFY FLIP_EVENT IS NOT NULL;
