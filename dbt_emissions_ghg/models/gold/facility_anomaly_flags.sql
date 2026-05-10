{{
    config(
        materialized='table'
    )
}}

WITH facility_percentiles AS (
    SELECT
        facility_id,
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY total_emissions_1000m3) AS p25_emissions,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY total_emissions_1000m3) AS p75_emissions,
        AVG(total_emissions_1000m3)                                          AS avg_emissions
    FROM {{ ref('monthly_emissions_correlation') }}
    WHERE total_emissions_1000m3 IS NOT NULL
    GROUP BY facility_id
)

SELECT
    m.facility_id,
    m.operator,
    m.facility_description,
    m.h3_cell,
    m.reporting_month,
    m.gas_flared_1000m3,
    m.gas_vented_1000m3,
    m.total_emissions_1000m3,
    m.monthly_avg_ch4,
    m.monthly_pixel_count,
    m.ch4_anomaly_score,
    CASE
        WHEN m.ch4_anomaly_score >  1.5
         AND m.total_emissions_1000m3 < p.p25_emissions
        THEN 'HIGH_CH4_LOW_REPORTED'
        WHEN m.ch4_anomaly_score < -1.5
         AND m.total_emissions_1000m3 > p.p75_emissions
        THEN 'LOW_CH4_HIGH_REPORTED'
        WHEN m.ch4_anomaly_score >  1.5
         AND m.total_emissions_1000m3 > p.p75_emissions
        THEN 'HIGH_CH4_HIGH_REPORTED'
        ELSE NULL
    END                         AS anomaly_type,
    m.monthly_avg_qa            AS satellite_qa
FROM {{ ref('monthly_emissions_correlation') }}     AS m
LEFT JOIN facility_percentiles                      AS p
    ON m.facility_id = p.facility_id
WHERE m.ch4_anomaly_score IS NOT NULL
  AND m.monthly_pixel_count >= {{ var('min_pixels_per_cell') }}
ORDER BY ABS(m.ch4_anomaly_score) DESC
