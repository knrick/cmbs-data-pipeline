{{
  config(
    materialized = 'table',
    post_hook = [
      "CREATE INDEX IF NOT EXISTS idx_{{ this.name }}_zip_code ON {{ this }} (zip_code)",
      "CREATE INDEX IF NOT EXISTS idx_{{ this.name }}_city_id ON {{ this }} (city_id)",
      "ANALYZE {{ this }}"
    ]
  )
}}

-- Extract unique zip codes from properties table, link to cities
WITH source_zips AS (
  SELECT DISTINCT
    property_zip AS zip_code,
    property_city AS city_name,
    property_state AS state_code
  FROM {{ source('cmbs', 'properties') }}
  WHERE 
    property_zip IS NOT NULL 
    AND property_zip != ''
    AND property_city IS NOT NULL 
    AND property_city != ''
    AND property_state IS NOT NULL
    AND property_state != ''
)

SELECT
  ROW_NUMBER() OVER (ORDER BY sz.state_code, sz.city_name, sz.zip_code) AS zip_id,
  sz.zip_code,
  c.city_id
FROM source_zips sz
JOIN {{ ref('dim_geo_city') }} c ON sz.city_name = c.city_name
JOIN {{ ref('dim_geo_state') }} s ON c.state_id = s.state_id AND sz.state_code = s.state_code 