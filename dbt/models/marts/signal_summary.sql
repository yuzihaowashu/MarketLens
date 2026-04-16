{{ config(alias='V_SIGNAL_SUMMARY') }}

-- Unified feed consumed by app/app.py and dags/marketlens_daily.py's
-- anomaly_check task. Every UNION block MUST produce exactly six columns in
-- this order:
--   DATE, SIGNAL_TYPE, ENTITY, MAGNITUDE, SALIENCE_SCORE, SUMMARY
-- Do not add extra columns without updating the downstream consumers.

-- Stock anomalies
SELECT
    DATE,
    'STOCK_ANOMALY' AS SIGNAL_TYPE,
    TICKER          AS ENTITY,
    ROUND(DAILY_RETURN * 100, 2) AS MAGNITUDE,
    ROUND(ABS(Z_SCORE), 2)       AS SALIENCE_SCORE,
    TICKER || ' moved ' || ROUND(DAILY_RETURN * 100, 2) || '% (z-score: '
        || ROUND(Z_SCORE, 2) || ')' AS SUMMARY
FROM {{ ref('anomaly_scores') }}
WHERE IS_ANOMALY = TRUE

UNION ALL

-- Fed rate changes (non-zero only)
SELECT
    DATE,
    'FED_RATE_CHANGE' AS SIGNAL_TYPE,
    'FED_FUNDS'       AS ENTITY,
    ROUND(RATE_CHANGE * 10000, 1)      AS MAGNITUDE,
    ABS(RATE_CHANGE) * 10000           AS SALIENCE_SCORE,
    'Fed Funds Rate changed by ' || ROUND(RATE_CHANGE * 10000, 1)
        || ' bps to ' || ROUND(FED_FUNDS_RATE * 100, 2) || '%' AS SUMMARY
FROM {{ ref('fed_rate_changes') }}
WHERE ABS(RATE_CHANGE) > 0.0001

UNION ALL

-- CPI month-over-month changes (>0.3% as notable)
SELECT
    DATE,
    'CPI_CHANGE'       AS SIGNAL_TYPE,
    'CPI_ALL_ITEMS'    AS ENTITY,
    ROUND(CPI_MOM_CHANGE * 100, 3)     AS MAGNITUDE,
    ABS(CPI_MOM_CHANGE) * 1000         AS SALIENCE_SCORE,
    'CPI changed ' || ROUND(CPI_MOM_CHANGE * 100, 3)
        || '% month-over-month to ' || ROUND(CPI_INDEX, 2) AS SUMMARY
FROM {{ ref('cpi_changes') }}
WHERE ABS(CPI_MOM_CHANGE) > 0.003

UNION ALL

-- SEC filings (narrative signals, one row per latest 10-K/10-Q per ticker)
SELECT
    FILING_DATE AS DATE,
    'SEC_FILING' AS SIGNAL_TYPE,
    TICKER       AS ENTITY,
    NULL         AS MAGNITUDE,
    CASE UPPER(COALESCE(MANAGEMENT_TONE, ''))
        WHEN 'NEGATIVE'  THEN 2.5
        WHEN 'CAUTIOUS'  THEN 1.5
        WHEN 'POSITIVE'  THEN 1.0
        ELSE 0.5
    END AS SALIENCE_SCORE,
    TICKER || ' filed ' || FORM_TYPE || ' (tone: '
        || COALESCE(MANAGEMENT_TONE, 'unknown') || ')' AS SUMMARY
FROM {{ ref('sec_narratives') }}
WHERE FILING_DATE IS NOT NULL

UNION ALL

-- RSI extremes (overbought / oversold)
SELECT
    DATE,
    'RSI_EXTREME'     AS SIGNAL_TYPE,
    TICKER            AS ENTITY,
    ROUND(RSI_14, 2)                AS MAGNITUDE,
    ROUND(ABS(RSI_14 - 50), 2)      AS SALIENCE_SCORE,
    TICKER || ' RSI(14)=' || ROUND(RSI_14, 1) || ' (' || RSI_STATE || ')' AS SUMMARY
FROM {{ ref('rsi_14') }}
WHERE RSI_STATE IN ('OVERBOUGHT', 'OVERSOLD')

UNION ALL

-- 50/200 SMA crossovers (golden / death)
SELECT
    DATE,
    'MA_CROSSOVER'    AS SIGNAL_TYPE,
    TICKER            AS ENTITY,
    ROUND(SMA_50 - SMA_200, 2)      AS MAGNITUDE,
    CASE CROSSOVER_EVENT WHEN 'DEATH_CROSS' THEN 3.5 ELSE 3.0 END AS SALIENCE_SCORE,
    TICKER || ' ' || CROSSOVER_EVENT || ' (SMA50=' || ROUND(SMA_50, 2)
        || ', SMA200=' || ROUND(SMA_200, 2) || ')' AS SUMMARY
FROM {{ ref('ma_crossover') }}
WHERE CROSSOVER_EVENT IS NOT NULL

UNION ALL

-- Drawdown from 52-week high (correction / bear)
SELECT
    DATE,
    'DRAWDOWN'        AS SIGNAL_TYPE,
    TICKER            AS ENTITY,
    ROUND(DRAWDOWN_PCT * 100, 2)       AS MAGNITUDE,
    ROUND(ABS(DRAWDOWN_PCT) * 10, 2)   AS SALIENCE_SCORE,
    TICKER || ' down ' || ROUND(DRAWDOWN_PCT * 100, 1)
        || '% from 52w high (' || DRAWDOWN_STATE || ')' AS SUMMARY
FROM {{ ref('drawdown') }}
WHERE DRAWDOWN_STATE IS NOT NULL

UNION ALL

-- Sector rotation: top and bottom sector by 20-day return per day
SELECT
    r.DATE,
    'SECTOR_ROTATION'  AS SIGNAL_TYPE,
    r.SECTOR           AS ENTITY,
    ROUND(r.AVG_20D_RETURN * 100, 2)      AS MAGNITUDE,
    ROUND(ABS(r.AVG_20D_RETURN) * 100, 2) AS SALIENCE_SCORE,
    r.SECTOR || CASE WHEN r.SECTOR_RANK = 1 THEN ' leading' ELSE ' lagging' END
        || ' sector: 20d avg return ' || ROUND(r.AVG_20D_RETURN * 100, 2) || '%' AS SUMMARY
FROM {{ ref('sector_rotation') }} r
JOIN (
    SELECT DATE, MAX(SECTOR_RANK) AS MAX_RANK
    FROM {{ ref('sector_rotation') }}
    GROUP BY DATE
) m ON m.DATE = r.DATE
WHERE r.SECTOR_RANK = 1 OR r.SECTOR_RANK = m.MAX_RANK

UNION ALL

-- Yield curve inversion flips (INVERSION_START / INVERSION_END)
SELECT
    DATE,
    'YIELD_CURVE'     AS SIGNAL_TYPE,
    'TREASURY_SPREAD' AS ENTITY,
    ROUND(CURVE_SPREAD * 100, 2)                                      AS MAGNITUDE,
    CASE FLIP_EVENT WHEN 'INVERSION_START' THEN 4.0 ELSE 3.0 END      AS SALIENCE_SCORE,
    CASE FLIP_EVENT
        WHEN 'INVERSION_START' THEN 'Yield curve inverted: 10Y-FFR spread '
            || ROUND(CURVE_SPREAD * 100, 2) || '% (recession warning)'
        WHEN 'INVERSION_END'   THEN 'Yield curve un-inverted: spread back to '
            || ROUND(CURVE_SPREAD * 100, 2) || '%'
    END AS SUMMARY
FROM {{ ref('yield_curve_signals') }}

UNION ALL

-- GDP contraction quarters
SELECT
    DATE,
    'GDP_CONTRACTION' AS SIGNAL_TYPE,
    'GDP_REAL'        AS ENTITY,
    QOQ_GROWTH_PCT                         AS MAGNITUDE,
    ROUND(ABS(QOQ_GROWTH_PCT), 2)          AS SALIENCE_SCORE,
    'Real GDP contracted ' || QOQ_GROWTH_PCT || '% QoQ to $'
        || ROUND(GDP_REAL_BILLIONS, 0) || 'B' AS SUMMARY
FROM {{ ref('gdp_changes') }}
WHERE IS_CONTRACTION = TRUE

UNION ALL

-- Consumer sentiment sharp moves (>5 points MoM)
SELECT
    DATE,
    'SENTIMENT_SHIFT' AS SIGNAL_TYPE,
    'UMICH_SENTIMENT' AS ENTITY,
    MOM_CHANGE                                                         AS MAGNITUDE,
    ROUND(ABS(MOM_CHANGE), 1)                                          AS SALIENCE_SCORE,
    'Consumer sentiment ' || CASE SENTIMENT_EVENT
        WHEN 'SHARP_DROP' THEN 'dropped sharply'
        WHEN 'SHARP_RISE' THEN 'rose sharply'
    END || ' by ' || MOM_CHANGE || ' pts to ' || SENTIMENT_INDEX AS SUMMARY
FROM {{ ref('sentiment_changes') }}
WHERE SENTIMENT_EVENT IS NOT NULL
