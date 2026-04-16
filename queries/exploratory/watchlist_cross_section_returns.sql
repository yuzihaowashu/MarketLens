-- Cross-section: latest day return for each watchlist ticker (exploratory only).
-- Adjust database/schema if your account differs.

USE DATABASE SCORPION_DB;
USE SCHEMA MARKETLENS;

WITH latest AS (
    SELECT MAX(date) AS d
    FROM V_ANOMALY_SCORES
)
SELECT
    a.ticker,
    a.date,
    ROUND(a.daily_return * 100, 4) AS daily_return_pct,
    ROUND(a.z_score, 4)          AS z_score
FROM V_ANOMALY_SCORES AS a
INNER JOIN latest AS l ON a.date = l.d
ORDER BY a.ticker;
