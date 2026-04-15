{{ config(alias='V_YIELD_CURVE') }}

-- 10Y Treasury minus Fed Funds Rate. Negative = inverted (recession signal).
-- Requires the paid marketplace share for stg_10y_treasury.
SELECT
    t.DATE,
    t.YIELD_PCT                          AS TREASURY_10Y,
    f.FED_FUNDS_RATE,
    t.YIELD_PCT - f.FED_FUNDS_RATE       AS CURVE_SPREAD,
    CASE WHEN t.YIELD_PCT - f.FED_FUNDS_RATE < 0 THEN TRUE ELSE FALSE END AS IS_INVERTED
FROM {{ ref('stg_10y_treasury') }} t
JOIN {{ ref('stg_fed_funds') }} f
  ON t.DATE = f.DATE
