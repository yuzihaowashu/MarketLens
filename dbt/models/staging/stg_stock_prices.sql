{{ config(alias='V_STOCK_PRICES') }}

-- Pivots the Marketplace timeseries (one row per VARIABLE) into a tidy OHLCV
-- shape keyed by TICKER + DATE.  Scoped to the watchlist declared in config.py.
SELECT
    TICKER,
    DATE,
    MAX(CASE WHEN VARIABLE = 'post-market_close_adjusted' THEN VALUE END) AS CLOSE_PRICE,
    MAX(CASE WHEN VARIABLE = 'pre-market_open_adjusted'   THEN VALUE END) AS OPEN_PRICE,
    MAX(CASE WHEN VARIABLE = 'all-day_high_adjusted'      THEN VALUE END) AS HIGH_PRICE,
    MAX(CASE WHEN VARIABLE = 'all-day_low_adjusted'       THEN VALUE END) AS LOW_PRICE,
    MAX(CASE WHEN VARIABLE = 'nasdaq_volume'              THEN VALUE END) AS VOLUME
FROM {{ source('snowflake_public_data_free', 'STOCK_PRICE_TIMESERIES') }}
WHERE TICKER IN ('AAPL','MSFT','GOOGL','AMZN','TSLA','NVDA','META','SPY','QQQ')
  AND VARIABLE IN (
      'post-market_close_adjusted','pre-market_open_adjusted',
      'all-day_high_adjusted','all-day_low_adjusted','nasdaq_volume'
  )
GROUP BY TICKER, DATE
