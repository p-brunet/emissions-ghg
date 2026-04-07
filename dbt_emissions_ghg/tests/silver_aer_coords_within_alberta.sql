-- dbt custom test: passes when 0 rows returned (no violations found).
-- Failure means Silver layer contains AER facilities outside the Alberta bounding box.
SELECT *
FROM {{ ref('aer_facilities_cleaned') }}
WHERE latitude  < {{ var('alberta_min_lat') }} OR latitude  > {{ var('alberta_max_lat') }}
   OR longitude < {{ var('alberta_min_lon') }} OR longitude > {{ var('alberta_max_lon') }}
