{{ config(alias='V_SEC_NARRATIVES') }}

-- Latest filing per (ticker, form_type).
WITH ranked AS (
    SELECT
        s.*,
        ROW_NUMBER() OVER (
            PARTITION BY s.TICKER, s.FORM_TYPE
            ORDER BY s.FILING_DATE DESC
        ) AS RN
    FROM {{ source('marketlens', 'SEC_FILING_SUMMARIES') }} s
)
SELECT
    TICKER,
    FORM_TYPE,
    FILING_DATE,
    ACCESSION_NUMBER,
    REVENUE_NARRATIVE,
    GUIDANCE_NARRATIVE,
    RISK_NARRATIVE,
    MANAGEMENT_TONE,
    EARNINGS_CONTEXT_SUMMARY,
    MODEL,
    SUMMARIZED_AT
FROM ranked
WHERE RN = 1
