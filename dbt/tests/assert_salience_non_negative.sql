-- SALIENCE_SCORE in the unified feed must be non-negative (magnitudes are ABS).
SELECT *
FROM {{ ref('signal_summary') }}
WHERE SALIENCE_SCORE IS NOT NULL
  AND SALIENCE_SCORE < 0
