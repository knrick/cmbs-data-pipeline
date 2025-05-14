{{
  config(
    materialized = 'incremental',
    unique_key = ['property_id', 'reporting_date', 'trust_id'],
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

WITH property_metrics_base AS (
    SELECT
        -- Generate surrogate key
        {{ dbt_utils.generate_surrogate_key(['p.asset_num', 'p.property_num', 'p.reporting_date', 'dt.trust_id']) }} AS property_metric_id,
        
        -- Base identifiers and metrics
        p.asset_num || '_' || p.property_num AS property_id,
        p.reporting_date,
        dt.trust_id,
        dp.property_type_code,
        dp.square_feet,
        dp.unit_count,
        p.current_valuation,
        p.securitization_valuation,
        p.current_occupancy_pct,
        p.securitization_occupancy_pct,
        p.current_revenue,
        p.current_expenses,
        p.current_noi,
        p.current_dscr,
        p.largest_tenant IS NOT NULL AS has_largest_tenant,
        p.second_largest_tenant IS NOT NULL AS has_second_largest_tenant,
        p.third_largest_tenant IS NOT NULL AS has_third_largest_tenant,
        dp.property_status_code,
        p.company AS issuer
    FROM {{ ref('int_properties_cleaned') }} p
    -- Join to dim_property
    JOIN {{ ref('dim_property') }} dp 
        ON p.asset_num || '_' || p.property_num = dp.property_id
        AND p.reporting_date >= dp.effective_date 
        AND p.reporting_date < dp.end_date
    JOIN {{ ref('dim_trust') }} dt 
        ON p.trust = dt.trust_name
    {% if is_incremental() %}
    WHERE p.reporting_date > (SELECT MAX(reporting_date) FROM {{ this }})
    {% endif %}
)

SELECT
    property_metric_id,
    property_id,
    reporting_date,
    trust_id,
    property_type_code,
    square_feet,
    unit_count,
    current_valuation,
    securitization_valuation,
    current_occupancy_pct,
    securitization_occupancy_pct,
    current_revenue,
    current_expenses,
    current_noi,
    current_dscr,
    has_largest_tenant,
    has_second_largest_tenant,
    has_third_largest_tenant,
    property_status_code,
    issuer,
    
    -- Valuation metrics
    CASE 
        WHEN securitization_valuation = 0
        THEN 0
        ELSE (current_valuation - securitization_valuation) / securitization_valuation 
    END AS valuation_change_pct,
    
    COALESCE(current_occupancy_pct, 0) - COALESCE(securitization_occupancy_pct, 0) AS occupancy_change_pct,
    
    CASE
        WHEN current_revenue = 0
        THEN 0
        ELSE current_expenses / current_revenue
    END AS expense_ratio,
    
    CASE 
        WHEN square_feet > 0 
        THEN current_revenue / square_feet 
        ELSE NULL 
    END AS revenue_per_sqft,
    
    CASE 
        WHEN unit_count > 0 
        THEN current_revenue / unit_count 
        ELSE NULL 
    END AS revenue_per_unit,

    CURRENT_TIMESTAMP AS loaded_at

FROM property_metrics_base 