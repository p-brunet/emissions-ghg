-- dbt custom test: passes when 0 rows returned (no violations found).
-- Failure means Gold hotspot averages are outside the physically valid CH4 range.
SELECT *
FROM {{ ref('regional_ch4_hotspots') }}
WHERE avg_ch4 < {{ var('ch4_min_valid') }}
   OR avg_ch4 > {{ var('ch4_max_valid') }}
