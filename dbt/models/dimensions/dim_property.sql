{{
  config(
    materialized = 'table',
    post_hook = [
      "CREATE INDEX IF NOT EXISTS idx_{{ this.name }}_property_id ON {{ this }} (property_id)",
      "CREATE INDEX IF NOT EXISTS idx_{{ this.name }}_property_type ON {{ this }} (property_type_code)",
      "CREATE INDEX IF NOT EXISTS idx_{{ this.name }}_property_state ON {{ this }} (property_state)",
      "ANALYZE {{ this }}"
    ]
  )
}}

-- Get the latest property record for each asset
WITH latest_property AS (
    SELECT 
        p.*,
        ROW_NUMBER() OVER (
            PARTITION BY asset_num 
            ORDER BY reporting_period_end_date DESC
        ) AS recency_rank
    FROM {{ source('cmbs', 'properties') }} p
)

SELECT
    -- Primary key
    asset_num AS property_id,
    
    -- Property details
    property_name,
    property_address,
    property_city,
    property_state,
    property_zip,
    property_county,
    
    -- Property characteristics
    property_type_code,
    
    -- Classification by property type
    CASE 
        WHEN property_type_code = 'RT' THEN 'Retail'
        WHEN property_type_code = 'OF' THEN 'Office'
        WHEN property_type_code = 'MF' THEN 'Multifamily'
        WHEN property_type_code = 'IN' THEN 'Industrial'
        WHEN property_type_code = 'HC' THEN 'Healthcare'
        WHEN property_type_code = 'LO' THEN 'Lodging/Hotel'
        WHEN property_type_code = 'MU' THEN 'Mixed Use'
        WHEN property_type_code = 'SS' THEN 'Self Storage'
        WHEN property_type_code = 'WH' THEN 'Warehouse'
        WHEN property_type_code = 'MH' THEN 'Mobile Home'
        ELSE 'Other'
    END AS property_type_description,
    
    -- Size metrics
    year_built_num AS year_built,
    units_beds_rooms_num AS unit_count,
    net_rentable_square_feet_num AS square_feet,
    
    -- Valuation
    most_recent_valuation_amt AS current_valuation,
    valuation_securitization_amt AS securitization_valuation,
    
    -- Occupancy
    most_recent_physical_occupancy_pct AS current_occupancy_pct,
    physical_occupancy_securitization_pct AS securitization_occupancy_pct,
    
    -- Status
    property_status_code,
    
    -- Tenant information
    largest_tenant,
    second_largest_tenant,
    third_largest_tenant,
    
    -- Financial metrics - most recent
    most_recent_revenue_amt AS current_revenue,
    COALESCE(operating_expenses_amt, 0) AS current_expenses,
    most_recent_net_operating_income_amt AS current_noi,
    most_recent_debt_service_amt AS current_dscr,
    
    -- Financial metrics - securitization
    revenue_securitization_amt AS securitization_revenue,
    operating_expenses_securitization_amt AS securitization_expenses,
    net_operating_income_securitization_amt AS securitization_noi,
    debt_service_coverage_net_operating_income_securitization_pct AS securitization_dscr,
    
    -- Metadata
    trust,
    company AS issuer,
    CURRENT_TIMESTAMP AS updated_at
FROM latest_property
WHERE recency_rank = 1 