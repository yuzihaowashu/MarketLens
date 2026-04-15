{{ config(alias='V_FED_FUNDS_RATE') }}

SELECT
    DATE,
    VALUE AS FED_FUNDS_RATE
FROM {{ source('snowflake_public_data_free', 'FINANCIAL_ECONOMIC_INDICATORS_TIMESERIES') }}
WHERE VARIABLE = 'EFFR_PCT'
  AND GEO_ID  = 'country/USA'
