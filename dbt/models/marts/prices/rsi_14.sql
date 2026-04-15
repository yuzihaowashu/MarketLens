{{ config(alias='V_RSI_14') }}

-- 14-day RSI using SMA of gains/losses (close enough to Wilder's EMA for
-- signal-generation purposes and avoids a recursive CTE).
WITH changes AS (
    SELECT
        TICKER,
        DATE,
        CLOSE_PRICE,
        CLOSE_PRICE - LAG(CLOSE_PRICE) OVER (PARTITION BY TICKER ORDER BY DATE) AS DELTA
    FROM {{ ref('stg_stock_prices') }}
),
gains_losses AS (
    SELECT
        TICKER,
        DATE,
        CLOSE_PRICE,
        CASE WHEN DELTA > 0 THEN DELTA ELSE 0 END  AS GAIN,
        CASE WHEN DELTA < 0 THEN -DELTA ELSE 0 END AS LOSS
    FROM changes
),
avg_gl AS (
    SELECT
        TICKER,
        DATE,
        CLOSE_PRICE,
        AVG(GAIN)  OVER (PARTITION BY TICKER ORDER BY DATE ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS AVG_GAIN_14,
        AVG(LOSS)  OVER (PARTITION BY TICKER ORDER BY DATE ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS AVG_LOSS_14,
        COUNT(*)   OVER (PARTITION BY TICKER ORDER BY DATE ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS WIN_N
    FROM gains_losses
)
SELECT
    TICKER,
    DATE,
    CLOSE_PRICE,
    CASE
        WHEN WIN_N < 14       THEN NULL
        WHEN AVG_LOSS_14 = 0  THEN 100
        ELSE 100 - (100 / (1 + (AVG_GAIN_14 / AVG_LOSS_14)))
    END AS RSI_14,
    CASE
        WHEN WIN_N < 14       THEN 'NEUTRAL'
        WHEN AVG_LOSS_14 = 0  THEN 'OVERBOUGHT'
        WHEN (100 - (100 / (1 + (AVG_GAIN_14 / NULLIF(AVG_LOSS_14, 0))))) > 70 THEN 'OVERBOUGHT'
        WHEN (100 - (100 / (1 + (AVG_GAIN_14 / NULLIF(AVG_LOSS_14, 0))))) < 30 THEN 'OVERSOLD'
        ELSE 'NEUTRAL'
    END AS RSI_STATE
FROM avg_gl
