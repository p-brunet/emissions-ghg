{{
    config(
        materialized='table'
    )
}}

WITH deduplicated AS (
    SELECT 
        facility_id,
        licence,
        operator,
        facility_description,
        reporting_month,
        latitude,
        longitude,
        gas_flared_1000m3,
        gas_vented_1000m3,
        total_wells,
        ROW_NUMBER() OVER (
            PARTITION BY facility_id 
            ORDER BY reporting_month DESC
        ) as rn
    FROM {{ source('bronze', 'aer_battery_monthly') }}
    WHERE latitude IS NOT NULL
      AND longitude IS NOT NULL
      AND latitude BETWEEN 49 AND 60
      AND longitude BETWEEN -120 AND -110
)

SELECT 
    facility_id,
    licence,
    operator,
    facility_description,
    reporting_month as latest_report_month,
    latitude,
    longitude,
    h3_latlng_to_cell(latitude, longitude, 6) as h3_cell,
    gas_flared_1000m3,
    gas_vented_1000m3,
    total_wells
FROM deduplicated
WHERE rn = 1