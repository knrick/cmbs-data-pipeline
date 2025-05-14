{{
  config(
    materialized = 'incremental',
    unique_key = ['asset_num', 'property_num', 'reporting_date'],
    partition_by = {
      "field": "reporting_date",
      "data_type": "date",
      "granularity": "month"
    },
    post_hook = [
      "CREATE INDEX IF NOT EXISTS idx_{{ this.name }}_asset_date ON {{ this }} (asset_num, reporting_date)",
      "CREATE INDEX IF NOT EXISTS idx_{{ this.name }}_property ON {{ this }} (asset_num, property_num)",
      "ANALYZE {{ this }}"
    ]
  )
}}

/*
 * Intermediate Properties Model
 * 
 * This model serves as the single source of truth for cleaned property data.
 * It handles:
 * 1. SCD fill-forward for financial metrics
 * 2. Split note (A1/A2) handling
 * 3. NULL value handling
 * 4. Data type standardization
 * 
 * All other models should reference this model instead of the raw properties source
 * to ensure consistent data cleaning and handling across the codebase.
 */

WITH property_data AS (
    SELECT DISTINCT ON (asset_num, property_num, reporting_period_end_date)
        -- Identifiers
        asset_num,
        property_num,
        reporting_period_end_date AS reporting_date,
        trust,
        company,
        
        -- Property characteristics
        property_name,
        property_address,
        {{ fill_scd_value('property_type_code') }} AS property_type_code,
        net_rentable_square_feet_num AS square_feet,
        units_beds_rooms_num AS unit_count,
        year_built_num AS year_built,
        {{ fill_scd_value('property_status_code') }} AS property_status_code,
        
        -- Location data
        {{ fill_scd_value('property_state') }} AS property_state,
        {{ fill_scd_value('property_city') }} AS property_city,
        {{ fill_scd_value('property_zip') }} AS property_zip,
        
        -- Valuation metrics
        {{ fill_scd_value('most_recent_valuation_amt') }} AS current_valuation,
        {{ fill_scd_value('valuation_securitization_amt') }} AS securitization_valuation,
        
        -- Occupancy metrics
        {{ fill_scd_value('most_recent_physical_occupancy_pct') }} AS current_occupancy_pct,
        {{ fill_scd_value('physical_occupancy_securitization_pct') }} AS securitization_occupancy_pct,
        
        -- Financial metrics with SCD fill-forward
        {{ fill_scd_value('most_recent_revenue_amt') }} AS current_revenue,
        {{ fill_scd_value('operating_expenses_amt') }} AS current_expenses,
        {{ fill_scd_value('most_recent_net_operating_income_amt') }} AS current_noi,
        {{ fill_scd_value('most_recent_debt_service_amt') }} AS current_dscr,
        
        -- Tenant information
        largest_tenant,
        second_largest_tenant,
        third_largest_tenant
    FROM {{ source('cmbs', 'properties') }}
    {% if is_incremental() %}
    WHERE reporting_period_end_date > (SELECT MAX(reporting_date) FROM {{ this }})
    {% endif %}
)

SELECT 
    p1.asset_num,
    p1.property_num,
    p1.reporting_date,
    p1.trust,
    p1.company,
    p1.property_name,
    
    -- Property characteristics with split note handling
    COALESCE(p1.property_type_code, p2.property_type_code) AS property_type_code,
    COALESCE(p1.square_feet, p2.square_feet) AS square_feet,
    COALESCE(p1.unit_count, p2.unit_count) AS unit_count,
    COALESCE(p1.year_built, p2.year_built) AS year_built,
    COALESCE(p1.property_status_code, p2.property_status_code) AS property_status_code,
    
    -- Location data
    COALESCE(p1.property_state, p2.property_state, 'UNK') AS property_state,
    COALESCE(p1.property_city, p2.property_city, 'Unknown') AS property_city,
    COALESCE(p1.property_zip, p2.property_zip, 'UNK') AS property_zip,
    COALESCE(p1.property_address, p2.property_address, 'Unknown') AS property_address,
    {{ dbt_utils.generate_surrogate_key([
        'COALESCE(p1.property_state, p2.property_state, \'UNK\')',
        'COALESCE(p1.property_city, p2.property_city, \'Unknown\')',
        'COALESCE(p1.property_zip, p2.property_zip, \'UNK\')',
        'COALESCE(p1.property_address, p2.property_address, \'Unknown\')'
    ]) }} AS geo_id,
    
    -- Valuation metrics
    COALESCE(p1.current_valuation, p2.current_valuation) AS current_valuation,
    COALESCE(p1.securitization_valuation, p2.securitization_valuation) AS securitization_valuation,
    
    -- Occupancy metrics
    COALESCE(p1.current_occupancy_pct, p2.current_occupancy_pct) AS current_occupancy_pct,
    COALESCE(p1.securitization_occupancy_pct, p2.securitization_occupancy_pct) AS securitization_occupancy_pct,
    
    -- Financial metrics (already SCD filled)
    COALESCE(p1.current_revenue, p2.current_revenue) AS current_revenue,
    COALESCE(p1.current_expenses, p2.current_expenses) AS current_expenses,
    COALESCE(p1.current_noi, p2.current_noi) AS current_noi,
    COALESCE(p1.current_dscr, p2.current_dscr) AS current_dscr,
    
    -- Tenant information
    COALESCE(p1.largest_tenant, p2.largest_tenant) AS largest_tenant,
    COALESCE(p1.second_largest_tenant, p2.second_largest_tenant) AS second_largest_tenant,
    COALESCE(p1.third_largest_tenant, p2.third_largest_tenant) AS third_largest_tenant,
    
    -- Metadata
    CURRENT_TIMESTAMP AS loaded_at
FROM property_data p1
LEFT JOIN property_data p2
    ON p1.asset_num = p2.asset_num || 'A'
    AND p1.property_num = p2.property_num
    AND p1.reporting_date = p2.reporting_date