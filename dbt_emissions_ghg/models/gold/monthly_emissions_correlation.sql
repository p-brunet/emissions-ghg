{{
    config(
        materialized='incremental',
        unique_key=['facility_id', 'reporting_month']
    )
}}

WITH satellite_monthly AS (
    SELECT
        h3_cell,
        DATE_TRUNC('month', measurement_date)   AS obs_month,
        COUNT(*)                                AS monthly_pixel_count,
        ROUND(AVG(ch4_column), 4)               AS monthly_avg_ch4,
        ROUND(STDDEV_POP(ch4_column), 4)        AS monthly_stddev_ch4,
        ROUND(AVG(qa_value), 4)                 AS monthly_avg_qa
    FROM {{ ref('sentinel5p_ch4_cleaned') }}
    {% if is_incremental() %}
    WHERE DATE_TRUNC('month', measurement_date) > (
        SELECT MAX(reporting_month) FROM {{ this }}
    )
    {% endif %}
    GROUP BY h3_cell, DATE_TRUNC('month', measurement_date)
),

cell_baselines AS (
    SELECT
        h3_cell,
        AVG(monthly_avg_ch4)    AS baseline_avg_ch4,
        STDDEV_POP(monthly_avg_ch4) AS baseline_stddev_ch4
    FROM satellite_monthly
    GROUP BY h3_cell
)

SELECT
    f.facility_id,
    f.licence,
    f.operator,
    f.facility_description,
    f.h3_cell,
    f.reporting_month,
    f.gas_flared_1000m3,
    f.gas_vented_1000m3,
    f.total_emissions_1000m3,
    f.total_wells,
    s.monthly_pixel_count,
    s.monthly_avg_ch4,
    s.monthly_stddev_ch4,
    s.monthly_avg_qa,
    CASE
        WHEN b.baseline_stddev_ch4 > 0
        THEN ROUND((s.monthly_avg_ch4 - b.baseline_avg_ch4) / b.baseline_stddev_ch4, 4)
        ELSE NULL
    END                         AS ch4_anomaly_score
FROM {{ ref('aer_facilities_monthly') }}    AS f
LEFT JOIN satellite_monthly                 AS s
    ON  f.h3_cell       = s.h3_cell
    AND f.reporting_month = s.obs_month
LEFT JOIN cell_baselines                    AS b
    ON  f.h3_cell       = b.h3_cell
