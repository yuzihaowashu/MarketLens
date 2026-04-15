-- Guard the 6-column contract consumed by app/app.py and the DAG.
-- If any row has a NULL DATE, SIGNAL_TYPE, ENTITY, or SUMMARY, the contract
-- is broken even though the individual models may pass their own tests.
SELECT *
FROM {{ ref('signal_summary') }}
WHERE DATE IS NULL
   OR SIGNAL_TYPE IS NULL
   OR ENTITY IS NULL
   OR SUMMARY IS NULL
