{{
    config(
        materialized='table'
    )
}}

SELECT 
    row_id,
    measurement_timestamp,
    ch4_column,
    ch4_column_precision,
    qa_value,
    latitude,
    longitude,
    orbit_number,
    h3_latlng_to_cell(latitude, longitude, 6) as h3_cell,
    DATE(measurement_timestamp) as measurement_date,
    EXTRACT(HOUR FROM measurement_timestamp) as measurement_hour
FROM {{ source('bronze', 'sentinel5p_raw') }}
WHERE qa_value >= 0.5
  AND ch4_column BETWEEN 1700 AND 2100
  AND latitude IS NOT NULL
  AND longitude IS NOT NULL