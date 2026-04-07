-- dbt custom test: passes when 0 rows returned (no violations found).
-- Failure means Silver layer contains CH4 values outside the physical valid range.
SELECT *
FROM {{ ref('sentinel5p_ch4_cleaned') }}
WHERE ch4_column < {{ var('ch4_min_valid') }}
   OR ch4_column > {{ var('ch4_max_valid') }}
