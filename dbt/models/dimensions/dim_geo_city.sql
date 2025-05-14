{{
  config(
    materialized = 'table',
    unique_key = 'city_id',
    post_hook = [
      "CREATE INDEX IF NOT EXISTS idx_{{ this.name }}_state_city ON {{ this }} (state_id, city_name)",
      "ANALYZE {{ this }}"
    ]
  )
}}

SELECT DISTINCT
    {{ dbt_utils.generate_surrogate_key(['s.state_id', 'p.property_city']) }} AS city_id,
    s.state_id,
    p.property_city AS city_name
FROM {{ ref('int_properties_cleaned') }} p
JOIN {{ ref('dim_geo_state') }} s 
    ON p.property_state = s.state_code