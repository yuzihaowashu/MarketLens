{{ config(alias='V_FRED_SENTIMENT') }}

-- University of Michigan Consumer Sentiment (UMCSENT) from FRED API.
-- Monthly index (1966 Q1 = 100). Ingested by FredProducer.
SELECT
    DATE,
    VALUE AS SENTIMENT_INDEX
FROM {{ source('marketlens', 'RAW_FRED_INDICATORS') }}
WHERE VARIABLE = 'CONSUMER_SENTIMENT'
  AND VALUE IS NOT NULL
  AND DATE >= DATEADD(YEAR, -5, CURRENT_DATE())
