-- FEDERAL FUNDS RATE
-- Quarterly from last 10 years
SELECT
    VARIABLE,
    DATE,
    VALUE
FROM SNOWFLAKE_PUBLIC_DATA_PAID.PUBLIC_DATA.FEDERAL_RESERVE_TIMESERIES_PIT
WHERE VARIABLE = 'Z1_FL072052006.Q'
  AND _EFFECTIVE_END_TIMESTAMP IS NULL
  AND DATE >= DATEADD(YEAR, -10, CURRENT_DATE)
ORDER BY DATE;

-- 10 Year Treasury Yield
-- Quarterly from last 10 years
SELECT
    VARIABLE,
    DATE,
    VALUE
FROM SNOWFLAKE_PUBLIC_DATA_PAID.PUBLIC_DATA.FEDERAL_RESERVE_TIMESERIES_PIT
WHERE VARIABLE = 'Z1_FL073161113.Q'
  AND _EFFECTIVE_END_TIMESTAMP IS NULL
  AND DATE >= DATEADD(YEAR, -10, CURRENT_DATE)
ORDER BY DATE;

-- Buera of Labor statistics un employment rate and growth in sectors
-- Pick some variables from this List
SELECT DISTINCT
    VARIABLE,
    VARIABLE_NAME
FROM SNOWFLAKE_PUBLIC_DATA_PAID.PUBLIC_DATA.BUREAU_OF_LABOR_STATISTICS_EMPLOYMENT_ATTRIBUTES
WHERE
      VARIABLE_NAME ILIKE '%employment%'
ORDER BY VARIABLE_NAME;

SELECT *
FROM BUREAU_OF_LABOR_STATISTICS_EMPLOYMENT_TIMESERIES
WHERE LOWER(series_title) LIKE '%unemployment rate%'
AND LOWER(series_title) LIKE '%missouri%';

-- Gets state and geographical ID for employment data
SELECT DISTINCT
    GEO_ID,
    GEO_NAME AS STATE_NAME
FROM SNOWFLAKE_PUBLIC_DATA_PAID.PUBLIC_DATA.GEOGRAPHY_RELATIONSHIPS_PIT
WHERE _EFFECTIVE_END_TIMESTAMP IS NULL
  AND LEVEL = 'State'
  AND GEO_NAME IN (
      'Alabama','Alaska','Arizona','Arkansas','California','Colorado','Connecticut',
      'Delaware','Florida','Georgia','Hawaii','Idaho','Illinois','Indiana','Iowa',
      'Kansas','Kentucky','Louisiana','Maine','Maryland','Massachusetts','Michigan',
      'Minnesota','Mississippi','Missouri','Montana','Nebraska','Nevada',
      'New Hampshire','New Jersey','New Mexico','New York','North Carolina',
      'North Dakota','Ohio','Oklahoma','Oregon','Pennsylvania','Rhode Island',
      'South Carolina','South Dakota','Tennessee','Texas','Utah','Vermont',
      'Virginia','Washington','West Virginia','Wisconsin','Wyoming'
  )
ORDER BY STATE_NAME;

-- Federal reserve deifferent stats
