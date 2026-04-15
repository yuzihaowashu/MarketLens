-- RSI must be in [0, 100] when defined.
SELECT *
FROM {{ ref('rsi_14') }}
WHERE RSI_14 IS NOT NULL
  AND (RSI_14 < 0 OR RSI_14 > 100)
