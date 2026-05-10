{{
    config(
        materialized='incremental',
        unique_key=['facility_id', 'reporting_month']
    )
}}

SELECT
    facility_id,
    licence,
    operator,
    facility_description,
    reporting_month,
    latitude,
    longitude,
    h3_latlng_to_cell(latitude, longitude, {{ var('h3_resolution') }}) AS h3_cell,
    gas_flared_1000m3,
    gas_vented_1000m3,
    gas_flared_1000m3 + gas_vented_1000m3                             AS total_emissions_1000m3,
    total_wells
FROM {{ source('bronze', 'aer_battery_monthly') }}
WHERE latitude  IS NOT NULL
  AND longitude IS NOT NULL
  AND gas_flared_1000m3 >= 0
  AND gas_vented_1000m3 >= 0
  AND latitude  BETWEEN {{ var('alberta_min_lat') }} AND {{ var('alberta_max_lat') }}
  AND longitude BETWEEN {{ var('alberta_min_lon') }} AND {{ var('alberta_max_lon') }}
{% if is_incremental() %}
  AND reporting_month > (SELECT MAX(reporting_month) FROM {{ this }})
{% endif %}
