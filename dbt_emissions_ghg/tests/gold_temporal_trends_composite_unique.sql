-- dbt custom test: passes when 0 rows returned (no violations found).
-- Failure means (measurement_date, h3_cell) is not unique — temporal trends model has duplicates.
SELECT measurement_date, h3_cell, COUNT(*) AS cnt
FROM {{ ref('temporal_ch4_trends') }}
GROUP BY measurement_date, h3_cell
HAVING cnt > 1
