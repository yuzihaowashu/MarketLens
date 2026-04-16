{{ config(alias='V_GDP_CHANGES') }}

-- Quarter-over-quarter GDP growth rate with contraction flag.
-- Two consecutive negatives = technical recession signal.
SELECT
    DATE,
    GDP_REAL_BILLIONS,
    LAG(GDP_REAL_BILLIONS) OVER (ORDER BY DATE) AS PREV_GDP,
    ROUND(
        (GDP_REAL_BILLIONS - LAG(GDP_REAL_BILLIONS) OVER (ORDER BY DATE))
        / NULLIF(LAG(GDP_REAL_BILLIONS) OVER (ORDER BY DATE), 0) * 100,
    2) AS QOQ_GROWTH_PCT,
    CASE
        WHEN GDP_REAL_BILLIONS < LAG(GDP_REAL_BILLIONS) OVER (ORDER BY DATE) THEN TRUE
        ELSE FALSE
    END AS IS_CONTRACTION
FROM {{ ref('stg_fred_gdp') }}
