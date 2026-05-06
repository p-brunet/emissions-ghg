{{
    config(
        materialized='incremental',
        unique_key='row_id'
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
    h3_latlng_to_cell(latitude, longitude, {{ var('h3_resolution') }}) as h3_cell,
    DATE(measurement_timestamp) as measurement_date,
    EXTRACT(HOUR FROM measurement_timestamp) as measurement_hour
FROM {{ source('bronze', 'sentinel5p_raw') }}
WHERE qa_value >= {{ var('qa_threshold_silver') }}
  AND ch4_column BETWEEN {{ var('ch4_min_valid') }} AND {{ var('ch4_max_valid') }}
  AND latitude IS NOT NULL
  AND longitude IS NOT NULL
{% if is_incremental() %}
  AND measurement_timestamp > (SELECT MAX(measurement_timestamp) FROM {{ this }})
{% endif %}