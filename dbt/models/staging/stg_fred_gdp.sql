{{ config(alias='V_FRED_GDP') }}

-- Real GDP (GDPC1) from FRED API, ingested by FredProducer.
-- Quarterly, seasonally adjusted annual rate (billions of chained 2017 dollars).
SELECT
    DATE,
    VALUE AS GDP_REAL_BILLIONS
FROM {{ source('marketlens', 'RAW_FRED_INDICATORS') }}
WHERE VARIABLE = 'GDP_REAL'
  AND VALUE IS NOT NULL
  AND DATE >= DATEADD(YEAR, -10, CURRENT_DATE())
