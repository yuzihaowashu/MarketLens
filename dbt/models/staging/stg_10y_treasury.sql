{{ config(alias='V_10Y_TREASURY') }}

-- Requires the SNOWFLAKE_PUBLIC_DATA_PAID marketplace subscription.
-- If unavailable, exclude this model with `--exclude stg_10y_treasury+`.
SELECT
    DATE,
    VALUE AS YIELD_PCT
FROM {{ source('snowflake_public_data_paid', 'FEDERAL_RESERVE_TIMESERIES_PIT') }}
WHERE VARIABLE = 'Z1_FL073161113.Q'
  AND _EFFECTIVE_END_TIMESTAMP IS NULL
  AND DATE >= DATEADD(YEAR, -10, CURRENT_DATE())
