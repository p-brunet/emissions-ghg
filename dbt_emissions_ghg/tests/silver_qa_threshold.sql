-- dbt custom test: passes when 0 rows returned (no violations found).
-- Failure means Silver layer contains rows below the QA threshold — filter is broken.
SELECT *
FROM {{ ref('sentinel5p_ch4_cleaned') }}
WHERE qa_value < {{ var('qa_threshold_silver') }}
