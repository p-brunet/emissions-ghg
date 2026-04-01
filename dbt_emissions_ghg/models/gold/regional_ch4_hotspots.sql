{{
    config(
        materialized='table'
    )
}}

SELECT
    h3_cell,
    COUNT(*)                              AS pixel_count,
    ROUND(AVG(ch4_column), 4)             AS avg_ch4,
    ROUND(STDDEV_POP(ch4_column), 4)      AS stddev_ch4,
    ROUND(MIN(ch4_column), 4)             AS min_ch4,
    ROUND(MAX(ch4_column), 4)             AS max_ch4,
    MIN(measurement_date)                 AS first_observation_date,
    MAX(measurement_date)                 AS last_observation_date,
    COUNT(DISTINCT measurement_date)      AS observation_days
FROM {{ ref('sentinel5p_ch4_cleaned') }}
GROUP BY h3_cell
HAVING COUNT(*) >= 5
ORDER BY avg_ch4 DESC
