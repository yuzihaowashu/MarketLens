{{ config(alias='V_SENTIMENT_CHANGES') }}

-- Month-over-month change in UMich Consumer Sentiment.
-- Flags sharp drops (>5 points) as a demand-slowdown warning.
SELECT
    DATE,
    SENTIMENT_INDEX,
    LAG(SENTIMENT_INDEX) OVER (ORDER BY DATE) AS PREV_SENTIMENT,
    ROUND(
        SENTIMENT_INDEX - LAG(SENTIMENT_INDEX) OVER (ORDER BY DATE),
    1) AS MOM_CHANGE,
    CASE
        WHEN SENTIMENT_INDEX - LAG(SENTIMENT_INDEX) OVER (ORDER BY DATE) <= -5 THEN 'SHARP_DROP'
        WHEN SENTIMENT_INDEX - LAG(SENTIMENT_INDEX) OVER (ORDER BY DATE) >= 5  THEN 'SHARP_RISE'
        ELSE NULL
    END AS SENTIMENT_EVENT
FROM {{ ref('stg_fred_sentiment') }}
