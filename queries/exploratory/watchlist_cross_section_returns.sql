-- Cross-section: latest day return for each watchlist ticker (exploratory only).
-- Adjust database/schema if your account differs.

USE DATABASE SCORPION_DB;
USE SCHEMA MARKETLENS;

WITH latest AS (
    SELECT MAX(DATE) AS d
    FROM V_ANOMALY_SCORES
)
SELECT
    a.TICKER,
    a.DATE,
    ROUND(a.DAILY_RETURN * 100, 4) AS daily_return_pct,
    ROUND(a.Z_SCORE, 4)            AS z_score
FROM V_ANOMALY_SCORES AS a
INNER JOIN latest AS l ON a.DATE = l.d
ORDER BY a.TICKER;
