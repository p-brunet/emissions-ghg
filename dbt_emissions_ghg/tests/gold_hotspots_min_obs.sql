-- dbt custom test: passes when 0 rows returned (no violations found).
-- Failure means Gold hotspot table contains cells below the minimum observation threshold.
-- This should be impossible given the HAVING clause, but validates the model logic.
SELECT *
FROM {{ ref('regional_ch4_hotspots') }}
WHERE pixel_count < {{ var('min_pixels_per_cell') }}
