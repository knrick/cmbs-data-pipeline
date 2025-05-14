{{
  config(
    materialized = 'table',
    unique_key = 'address_id',
    post_hook = [
      "CREATE INDEX IF NOT EXISTS idx_{{ this.name }}_zip ON {{ this }} (zip_id)",
      "ANALYZE {{ this }}"
    ]
  )
}}

SELECT DISTINCT
    p.geo_id AS address_id,  -- Using the pre-calculated hierarchical ID
    z.zip_id,
    p.property_address
FROM {{ ref('int_properties_cleaned') }} p
JOIN {{ ref('dim_geo_state') }} s 
    ON p.property_state = s.state_code
JOIN {{ ref('dim_geo_city') }} c 
    ON p.property_city = c.city_name
    AND s.state_id = c.state_id
JOIN {{ ref('dim_geo_zip') }} z 
    ON p.property_zip = z.zip_code
    AND c.city_id = z.city_id