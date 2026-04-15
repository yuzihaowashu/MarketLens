USE SCHEMA SCORPION_DB.MARKETLENS;

-- Average 20-day return per sector, ranked best-to-worst per day.
-- Excludes 'Index' bucket (SPY/QQQ) so they don't dominate the sector view.
CREATE OR REPLACE VIEW V_SECTOR_ROTATION AS
WITH ret_20d AS (
    SELECT
        p.TICKER,
        p.DATE,
        p.CLOSE_PRICE,
        LAG(p.CLOSE_PRICE, 20) OVER (PARTITION BY p.TICKER ORDER BY p.DATE) AS PRICE_20D_AGO
    FROM V_STOCK_PRICES p
),
joined AS (
    SELECT
        s.SECTOR,
        r.DATE,
        (r.CLOSE_PRICE - r.PRICE_20D_AGO) / NULLIF(r.PRICE_20D_AGO, 0) AS RET_20D
    FROM ret_20d r
    JOIN DIM_TICKER_SECTOR s ON s.TICKER = r.TICKER
    WHERE r.PRICE_20D_AGO IS NOT NULL
      AND s.SECTOR <> 'Index'
),
agg AS (
    SELECT
        DATE,
        SECTOR,
        AVG(RET_20D) AS AVG_20D_RETURN
    FROM joined
    GROUP BY DATE, SECTOR
)
SELECT
    DATE,
    SECTOR,
    AVG_20D_RETURN,
    DENSE_RANK() OVER (PARTITION BY DATE ORDER BY AVG_20D_RETURN DESC) AS SECTOR_RANK
FROM agg;
