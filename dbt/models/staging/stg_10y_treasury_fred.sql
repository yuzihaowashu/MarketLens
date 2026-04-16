{{ config(alias='V_10Y_TREASURY_FRED') }}

-- 10Y Treasury constant maturity yield sourced from FRED (DGS10).
-- Free alternative to stg_10y_treasury which requires the paid marketplace.
-- Populated by FredProducer (ingestion/fred_producer.py).
SELECT
    DATE,
    VALUE / 100.0 AS YIELD_PCT   -- FRED stores as percent (e.g. 4.25), normalise to decimal
FROM {{ source('marketlens', 'RAW_FRED_INDICATORS') }}
WHERE VARIABLE = 'TREASURY_10Y'
  AND VALUE IS NOT NULL
  AND DATE >= DATEADD(YEAR, -10, CURRENT_DATE())
