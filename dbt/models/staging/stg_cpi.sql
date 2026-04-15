{{ config(alias='V_CPI') }}

SELECT
    DATE,
    VALUE AS CPI_INDEX
FROM {{ source('snowflake_public_data_free', 'BUREAU_OF_LABOR_STATISTICS_PRICE_TIMESERIES') }}
WHERE VARIABLE = 'CPI:_All_items,_Seasonally_adjusted,_Monthly'
  AND GEO_ID  = 'country/USA'
