{{
  config(
    materialized = 'incremental',
    unique_key = ['property_id', 'reporting_date', 'trust_id', 'source_file'],
    partition_by = {
      "field": "reporting_date",
      "data_type": "date",
      "granularity": "month"
    },
    post_hook = [
      "CREATE INDEX IF NOT EXISTS idx_{{ this.name }}_prop_date ON {{ this }} (property_id, reporting_date)",
      "CREATE INDEX IF NOT EXISTS idx_{{ this.name }}_trust_date ON {{ this }} (trust_id, reporting_date)",
      "ANALYZE {{ this }}"
    ]
  )
}}

WITH property_data AS (
    SELECT
        p.asset_num AS property_id,
        p.reporting_period_end_date AS reporting_date,
        p.trust AS trust_id,
        
        -- Location info for joining to geography dimension
        p.property_state,
        p.property_city,
        p.property_zip,
        
        -- Property characteristics
        p.property_type_code,
        p.net_rentable_square_feet_num AS square_feet,
        p.units_beds_rooms_num AS unit_count,
        
        -- Valuation metrics
        p.most_recent_valuation_amt AS current_valuation,
        p.valuation_securitization_amt AS securitization_valuation,
        
        -- Occupancy metrics
        p.most_recent_physical_occupancy_pct AS current_occupancy_pct,
        p.physical_occupancy_securitization_pct AS securitization_occupancy_pct,
        
        -- Financial metrics
        p.most_recent_revenue_amt AS current_revenue,
        COALESCE(p.operating_expenses_amt, 0) AS current_expenses,
        p.most_recent_net_operating_income_amt AS current_noi,
        p.most_recent_debt_service_amt AS current_dscr,
        
        -- Tenant metrics
        p.largest_tenant IS NOT NULL AS has_largest_tenant,
        p.second_largest_tenant IS NOT NULL AS has_second_largest_tenant,
        p.third_largest_tenant IS NOT NULL AS has_third_largest_tenant,
        
        -- Status
        p.property_status_code,
        
        -- Metadata
        p.company AS issuer,
        p.source_file
    FROM {{ source('cmbs', 'properties') }} p
    WHERE p.reporting_period_end_date IS NOT NULL
    
    {% if is_incremental() %}
      -- Only process new data since last run
      AND p.reporting_period_end_date > (
        SELECT COALESCE(MAX(reporting_date), '2000-01-01'::date) FROM {{ this }}
      )
    {% endif %}
),

-- Get geography ID
geo_data AS (
    SELECT
        geo_id,
        state,
        city,
        zip_code
    FROM {{ ref('dim_geography') }}
)

SELECT
    -- Generate surrogate key using dbt_utils
    {{ dbt_utils.generate_surrogate_key(['pd.property_id', 'pd.reporting_date', 'pd.trust_id', 'pd.source_file']) }} AS property_metric_id,
    
    -- Foreign keys for dimension tables
    pd.property_id,
    pd.reporting_date,
    pd.trust_id,
    gd.geo_id,
    
    -- Property characteristics
    pd.property_type_code,
    pd.square_feet,
    pd.unit_count,
    
    -- Valuation metrics
    pd.current_valuation,
    pd.securitization_valuation,
    CASE 
        WHEN pd.securitization_valuation > 0 
        THEN (pd.current_valuation - pd.securitization_valuation) / pd.securitization_valuation 
        ELSE NULL 
    END AS valuation_change_pct,
    
    -- Occupancy metrics
    pd.current_occupancy_pct,
    pd.securitization_occupancy_pct,
    COALESCE(pd.current_occupancy_pct, 0) - COALESCE(pd.securitization_occupancy_pct, 0) AS occupancy_change_pct,
    
    -- Financial metrics
    pd.current_revenue,
    pd.current_expenses,
    pd.current_noi,
    pd.current_dscr,
    CASE 
        WHEN pd.current_revenue > 0 
        THEN pd.current_expenses / pd.current_revenue 
        ELSE NULL 
    END AS expense_ratio,
    
    -- Calculated revenue metrics
    CASE 
        WHEN pd.square_feet > 0 
        THEN pd.current_revenue / pd.square_feet 
        ELSE NULL 
    END AS revenue_per_sqft,
    
    CASE 
        WHEN pd.unit_count > 0 
        THEN pd.current_revenue / pd.unit_count 
        ELSE NULL 
    END AS revenue_per_unit,
    
    -- Tenant metrics
    pd.has_largest_tenant,
    pd.has_second_largest_tenant,
    pd.has_third_largest_tenant,
    
    -- Status
    pd.property_status_code,
    
    -- Source file for tracking
    pd.source_file,
    
    -- Timestamp
    CURRENT_TIMESTAMP AS loaded_at
FROM property_data pd
LEFT JOIN geo_data gd ON 
    pd.property_state = gd.state AND
    pd.property_city = gd.city AND
    pd.property_zip = gd.zip_code 