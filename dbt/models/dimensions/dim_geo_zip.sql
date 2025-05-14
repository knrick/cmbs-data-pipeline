{{
  config(
    materialized = 'table',
    unique_key = 'zip_id',
    post_hook = [
      "CREATE INDEX IF NOT EXISTS idx_{{ this.name }}_city_zip ON {{ this }} (city_id, zip_code)",
      "ANALYZE {{ this }}"
    ]
  )
}}

SELECT DISTINCT
    {{ dbt_utils.generate_surrogate_key(['c.city_id', 'p.property_zip']) }} AS zip_id,
    c.city_id,
    p.property_zip AS zip_code
FROM {{ ref('int_properties_cleaned') }} p
JOIN {{ ref('dim_geo_state') }} s 
    ON p.property_state = s.state_code
JOIN {{ ref('dim_geo_city') }} c 
    ON p.property_city = c.city_name
    AND s.state_id = c.state_id