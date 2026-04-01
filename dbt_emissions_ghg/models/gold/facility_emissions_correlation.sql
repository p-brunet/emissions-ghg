{{
    config(
        materialized='table'
    )
}}

SELECT
    f.facility_id,
    f.licence,
    f.operator,
    f.facility_description,
    f.h3_cell,
    f.gas_flared_1000m3,
    f.gas_vented_1000m3,
    f.gas_flared_1000m3 + f.gas_vented_1000m3  AS total_emissions_1000m3,
    f.total_wells,
    f.latest_report_month,
    COUNT(s.row_id)                             AS satellite_obs_count,
    ROUND(AVG(s.ch4_column), 4)                 AS avg_ch4_column,
    ROUND(STDDEV_POP(s.ch4_column), 4)          AS stddev_ch4_column,
    ROUND(MIN(s.ch4_column), 4)                 AS min_ch4_column,
    ROUND(MAX(s.ch4_column), 4)                 AS max_ch4_column,
    MIN(s.measurement_date)                     AS first_obs_date,
    MAX(s.measurement_date)                     AS last_obs_date
FROM {{ ref('aer_facilities_cleaned') }} AS f
LEFT JOIN {{ ref('sentinel5p_ch4_cleaned') }} AS s
    ON f.h3_cell = s.h3_cell
GROUP BY
    f.facility_id,
    f.licence,
    f.operator,
    f.facility_description,
    f.h3_cell,
    f.gas_flared_1000m3,
    f.gas_vented_1000m3,
    f.total_wells,
    f.latest_report_month
ORDER BY avg_ch4_column DESC NULLS LAST
