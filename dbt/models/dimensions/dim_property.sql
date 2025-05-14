{{
  config(
    materialized = 'table',
    unique_key = ['property_id', 'effective_date'],
    post_hook = [
      "CREATE INDEX IF NOT EXISTS idx_{{ this.name }}_property_dates ON {{ this }} (property_id, effective_date, end_date)",
      "CREATE INDEX IF NOT EXISTS idx_{{ this.name }}_property_type ON {{ this }} (property_type_code)",
      "CREATE INDEX IF NOT EXISTS idx_{{ this.name }}_geo ON {{ this }} (geo_id)",
      "CREATE INDEX IF NOT EXISTS idx_{{ this.name }}_trust ON {{ this }} (trust_id)",
      "ANALYZE {{ this }}"
    ]
  )
}}

WITH property_changes AS (
    SELECT
        -- Identifiers and dates
        asset_num || '_' || property_num AS property_id,
        reporting_date AS effective_date,
        
        -- Property attributes to track
        property_name,
        property_type_code,
        property_status_code,
        geo_id,
        year_built,
        square_feet,
        unit_count,
        trust,
        company AS issuer,
        
        -- Track if key attributes changed from previous values
        COALESCE(
            LAG(property_type_code) OVER (
                PARTITION BY asset_num, property_num 
                ORDER BY reporting_date
            ),
            'first_row'
        ) != property_type_code AS property_type_changed,
        
        COALESCE(
            LAG(property_status_code) OVER (
                PARTITION BY asset_num, property_num 
                ORDER BY reporting_date
            ),
            'first_row'
        ) != property_status_code AS property_status_changed,
        
        COALESCE(
            LAG(square_feet) OVER (
                PARTITION BY asset_num, property_num 
                ORDER BY reporting_date
            ),
            -1
        ) != square_feet AS square_feet_changed,
        
        COALESCE(
            LAG(unit_count) OVER (
                PARTITION BY asset_num, property_num 
                ORDER BY reporting_date
            ),
            -1
        ) != unit_count AS unit_count_changed,
        
        COALESCE(
            LAG(geo_id) OVER (
                PARTITION BY asset_num, property_num 
                ORDER BY reporting_date
            ),
            'first_row'
        ) != geo_id AS geo_id_changed,
        
        COALESCE(
            LAG(trust) OVER (
                PARTITION BY asset_num, property_num 
                ORDER BY reporting_date
            ),
            'first_row'
        ) != trust AS trust_changed
    FROM {{ ref('int_properties_cleaned') }}
),

versioned_properties AS (
    SELECT 
        property_id,
        effective_date,
        property_name,
        property_type_code,
        property_status_code,
        geo_id,
        year_built,
        square_feet,
        unit_count,
        trust,
        issuer
    FROM property_changes
    WHERE 
        -- Only create new version when key attributes change
        property_type_changed OR
        property_status_changed OR
        square_feet_changed OR
        unit_count_changed OR
        geo_id_changed OR
        trust_changed
)

SELECT
    -- SCD Type 2 fields
    vp.property_id,
    vp.effective_date,
    COALESCE(
        LEAD(vp.effective_date) OVER (
            PARTITION BY vp.property_id 
            ORDER BY vp.effective_date
        ),
        '9999-12-31'::date
    ) AS end_date,
    
    -- Property details
    vp.property_name,
    vp.property_type_code,
    -- Classification by property type
    CASE 
        WHEN vp.property_type_code = 'RT' THEN 'Retail'
        WHEN vp.property_type_code = 'OF' THEN 'Office'
        WHEN vp.property_type_code = 'MF' THEN 'Multifamily'
        WHEN vp.property_type_code = 'IN' THEN 'Industrial'
        WHEN vp.property_type_code = 'HC' THEN 'Healthcare'
        WHEN vp.property_type_code = 'LO' THEN 'Lodging/Hotel'
        WHEN vp.property_type_code = 'MU' THEN 'Mixed Use'
        WHEN vp.property_type_code = 'SS' THEN 'Self Storage'
        WHEN vp.property_type_code = 'WH' THEN 'Warehouse'
        WHEN vp.property_type_code = 'MH' THEN 'Mobile Home'
        ELSE 'Other'
    END AS property_type_description,
    vp.property_status_code,
    vp.year_built,
    vp.square_feet,
    vp.unit_count,
    vp.issuer,
    
    -- Foreign keys
    dt.trust_id,
    vp.geo_id
FROM versioned_properties vp
JOIN {{ ref('dim_trust') }} dt ON vp.trust = dt.trust_name 