-- dbt custom test: passes when 0 rows returned (no violations found).
-- Failure means Silver layer contains negative gas volumes — data quality issue.
SELECT *
FROM {{ ref('aer_facilities_cleaned') }}
WHERE gas_flared_1000m3 < 0
   OR gas_vented_1000m3 < 0
