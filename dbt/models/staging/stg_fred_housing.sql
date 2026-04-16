{{ config(alias='V_FRED_HOUSING') }}

-- Housing Starts (HOUST) from FRED API, ingested by FredProducer.
-- Monthly, seasonally adjusted annual rate (thousands of units).
SELECT
    DATE,
    VALUE AS HOUSING_STARTS_K
FROM {{ source('marketlens', 'RAW_FRED_INDICATORS') }}
WHERE VARIABLE = 'HOUSING_STARTS'
  AND VALUE IS NOT NULL
  AND DATE >= DATEADD(YEAR, -5, CURRENT_DATE())
