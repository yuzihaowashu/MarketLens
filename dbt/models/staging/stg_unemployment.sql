{{ config(alias='V_UNEMPLOYMENT_RATE') }}

-- Requires the SNOWFLAKE_PUBLIC_DATA_PAID marketplace subscription.
SELECT
    DATE,
    VALUE         AS UNEMPLOYMENT_PCT,
    GEO_ID,
    SERIES_TITLE
FROM {{ source('snowflake_public_data_paid', 'BUREAU_OF_LABOR_STATISTICS_EMPLOYMENT_TIMESERIES') }}
WHERE LOWER(SERIES_TITLE) LIKE '%unemployment rate%'
  AND LOWER(SERIES_TITLE) NOT LIKE '%not seasonally%'
  AND GEO_ID  = 'country/USA'
