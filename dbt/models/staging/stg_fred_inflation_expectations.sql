{{ config(alias='V_FRED_INFLATION_EXPECTATIONS') }}

-- 10-Year Breakeven Inflation Rate (T10YIE) from FRED API.
-- Daily. Difference between 10Y nominal Treasury and 10Y TIPS yield.
-- Proxy for market-implied long-run inflation expectations.
SELECT
    DATE,
    VALUE / 100.0 AS INFLATION_EXPECTATION_PCT
FROM {{ source('marketlens', 'RAW_FRED_INDICATORS') }}
WHERE VARIABLE = 'INFLATION_EXPECTATIONS_10Y'
  AND VALUE IS NOT NULL
  AND DATE >= DATEADD(YEAR, -5, CURRENT_DATE())
