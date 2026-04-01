{{
    config(
        materialized='table'
    )
}}

SELECT
    measurement_date,
    h3_cell,
    COUNT(*)                               AS daily_pixel_count,
    ROUND(AVG(ch4_column), 4)              AS daily_avg_ch4,
    ROUND(STDDEV_POP(ch4_column), 4)       AS daily_stddev_ch4,
    ROUND(MIN(ch4_column), 4)              AS daily_min_ch4,
    ROUND(MAX(ch4_column), 4)              AS daily_max_ch4,
    ROUND(AVG(ch4_column_precision), 4)    AS avg_precision,
    ROUND(AVG(qa_value), 4)                AS avg_qa_value,
    MIN(orbit_number)                      AS first_orbit,
    MAX(orbit_number)                      AS last_orbit
FROM {{ ref('sentinel5p_ch4_cleaned') }}
GROUP BY measurement_date, h3_cell
ORDER BY measurement_date, h3_cell
