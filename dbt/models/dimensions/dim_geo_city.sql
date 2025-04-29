{{
  config(
    materialized = 'table',
    post_hook = [
      "CREATE INDEX IF NOT EXISTS idx_{{ this.name }}_city_name ON {{ this }} (city_name)",
      "CREATE INDEX IF NOT EXISTS idx_{{ this.name }}_state_id ON {{ this }} (state_id)",
      "ANALYZE {{ this }}"
    ]
  )
}}

-- Extract unique cities from properties table, link to states
WITH source_cities AS (
  SELECT DISTINCT
    property_city AS city_name,
    property_state AS state_code
  FROM {{ source('cmbs', 'properties') }}
  WHERE 
    property_city IS NOT NULL 
    AND property_city != ''
    AND property_state IS NOT NULL
    AND property_state != ''
)

SELECT
  ROW_NUMBER() OVER (ORDER BY sc.state_code, sc.city_name) AS city_id,
  sc.city_name,
  s.state_id
FROM source_cities sc 
JOIN {{ ref('dim_geo_state') }} s ON sc.state_code = s.state_code 