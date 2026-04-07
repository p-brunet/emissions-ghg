-- dbt custom test: passes when 0 rows returned (no violations found).
-- Failure means Silver layer contains satellite pixels outside the Alberta bounding box.
SELECT *
FROM {{ ref('sentinel5p_ch4_cleaned') }}
WHERE latitude  < {{ var('alberta_min_lat') }} OR latitude  > {{ var('alberta_max_lat') }}
   OR longitude < {{ var('alberta_min_lon') }} OR longitude > {{ var('alberta_max_lon') }}
