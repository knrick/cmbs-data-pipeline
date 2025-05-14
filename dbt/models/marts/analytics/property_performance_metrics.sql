{{
    config(
        materialized='incremental',
        unique_key=['property_id', 'reporting_date'],
        partition_by={
            'field': 'reporting_date',
            'data_type': 'date',
            'granularity': 'month'
        },
        post_hook = [
            "CREATE INDEX IF NOT EXISTS idx_{{ this.name }}_loan_date ON {{ this }} (loan_id, reporting_date)",
            "CREATE INDEX IF NOT EXISTS idx_{{ this.name }}_property_date ON {{ this }} (property_id, reporting_date)",
            "CREATE INDEX IF NOT EXISTS idx_{{ this.name }}_property_type ON {{ this }} (property_type_code, reporting_date)",
            "CREATE INDEX IF NOT EXISTS idx_{{ this.name }}_geo ON {{ this }} (state_code, reporting_date)",
            "ANALYZE {{ this }}"
        ]
    )
}}

/*
 * Property Performance Metrics Model
 * 
 * This model combines property metrics with loan allocations to provide a comprehensive
 * view of property performance. Key features:
 * - Loan balance allocation based on square footage or equal distribution
 * - Property-level risk metrics including LTV and debt yield
 * - Geographic and property type benchmarking
 * - Trend analysis and performance categorization
 *
 * Incremental Processing:
 * - Processes new data monthly as it arrives
 * - Updates existing records if source data changes
 * - Maintains historical trends while processing incrementally
 */

WITH loan_property_allocation AS (
    SELECT
        dp.property_id,
        lp.loan_id,
        lp.reporting_date,
        lp.current_bal AS loan_balance,
        dp.square_feet,
        -- Calculate total square feet per loan
        SUM(dp.square_feet) OVER (
            PARTITION BY lp.loan_id, lp.reporting_date
        ) AS total_loan_square_feet,
        -- Calculate number of properties per loan
        COUNT(*) OVER (
            PARTITION BY lp.loan_id, lp.reporting_date
        ) AS total_loan_properties,
        -- Calculate allocation factor
        COALESCE(
            dp.square_feet / NULLIF(SUM(dp.square_feet) OVER (
                PARTITION BY lp.loan_id, lp.reporting_date
            ), 0),
            1.0 / COUNT(*) OVER (
                PARTITION BY lp.loan_id, lp.reporting_date
            )
        ) AS allocation_factor
    FROM {{ ref('fct_loan_monthly_performance') }} lp
    -- Join to property dimension
    JOIN {{ ref('dim_property') }} dp 
        ON dp.property_id = lp.loan_id
        AND lp.reporting_date >= dp.effective_date 
        AND lp.reporting_date < dp.end_date
    WHERE lp.reporting_date IS NOT NULL
    {% if is_incremental() %}
    AND lp.reporting_date >= (SELECT max(reporting_date) FROM {{ this }})
    {% endif %}
),

property_metrics AS (
    SELECT
        pm.property_id,
        pm.reporting_date,
        pm.current_valuation,
        pm.securitization_valuation,
        pm.current_occupancy_pct,
        pm.securitization_occupancy_pct,
        pm.current_revenue,
        pm.current_expenses,
        pm.current_noi,
        pm.current_dscr,
        dp.property_type_code,
        dp.square_feet,
        dp.unit_count,
        -- Geographic attributes from snowflake schema
        s.state_code,
        s.state_name,
        s.region_name,
        s.division_name,
        c.city_name,
        z.zip_code
    FROM {{ ref('fct_property_metrics') }} pm
    JOIN {{ ref('dim_property') }} dp 
        ON pm.property_id = dp.property_id
        AND pm.reporting_date >= dp.effective_date 
        AND pm.reporting_date < dp.end_date
    -- Geographic snowflake joins
    LEFT JOIN {{ ref('dim_geo_address') }} a ON dp.geo_id = a.address_id
    LEFT JOIN {{ ref('dim_geo_zip') }} z ON a.zip_id = z.zip_id
    LEFT JOIN {{ ref('dim_geo_city') }} c ON z.city_id = c.city_id
    LEFT JOIN {{ ref('dim_geo_state') }} s ON c.state_id = s.state_id
    WHERE 1=1
    {% if is_incremental() %}
    AND pm.reporting_date >= (SELECT max(reporting_date) FROM {{ this }})
    {% endif %}
),

property_trends AS (
    SELECT
        property_id,
        reporting_date,
        -- 3-month trends
        current_valuation - LAG(current_valuation, 3) OVER (
            PARTITION BY property_id ORDER BY reporting_date
        ) AS valuation_3m_change,
        current_occupancy_pct - LAG(current_occupancy_pct, 3) OVER (
            PARTITION BY property_id ORDER BY reporting_date
        ) AS occupancy_3m_change,
        current_noi - LAG(current_noi, 3) OVER (
            PARTITION BY property_id ORDER BY reporting_date
        ) AS noi_3m_change,
        -- 12-month trends
        current_valuation - LAG(current_valuation, 12) OVER (
            PARTITION BY property_id ORDER BY reporting_date
        ) AS valuation_12m_change,
        current_occupancy_pct - LAG(current_occupancy_pct, 12) OVER (
            PARTITION BY property_id ORDER BY reporting_date
        ) AS occupancy_12m_change,
        current_noi - LAG(current_noi, 12) OVER (
            PARTITION BY property_id ORDER BY reporting_date
        ) AS noi_12m_change
    FROM property_metrics
),

type_benchmarks AS (
    SELECT
        reporting_date,
        property_type_code,
        AVG(current_occupancy_pct) AS type_avg_occupancy_pct,
        AVG(current_valuation / NULLIF(square_feet, 0)) AS type_avg_value_per_sq_ft,
        AVG(current_revenue / NULLIF(square_feet, 0)) AS type_avg_revenue_per_sq_ft,
        AVG(current_noi / NULLIF(square_feet, 0)) AS type_avg_noi_per_sq_ft,
        AVG(current_expenses / NULLIF(current_revenue, 0)) AS type_avg_expense_ratio,
        AVG(current_dscr) AS type_avg_dscr
    FROM property_metrics
    WHERE property_type_code IS NOT NULL
    GROUP BY reporting_date, property_type_code
)

SELECT
    -- Property and loan identifiers
    pm.property_id,
    lpa.loan_id,
    pm.reporting_date,
    
    -- Loan allocation metrics
    lpa.loan_balance,
    lpa.allocation_factor,
    lpa.loan_balance * lpa.allocation_factor AS allocated_loan_balance,
    lpa.total_loan_properties,
    lpa.total_loan_square_feet,
    
    -- Property characteristics
    pm.property_type_code,
    pm.square_feet,
    pm.unit_count,
    
    -- Geographic attributes
    pm.zip_code,
    pm.city_name,
    pm.state_code,
    pm.state_name,
    pm.region_name,
    pm.division_name,
    
    -- Core property metrics
    pm.current_valuation,
    pm.securitization_valuation,
    pm.current_occupancy_pct,
    pm.securitization_occupancy_pct,
    pm.current_revenue,
    pm.current_expenses,
    pm.current_noi,
    pm.current_dscr,
    
    -- Calculated metrics
    CASE 
        WHEN pm.current_valuation > 0 
        THEN (lpa.loan_balance * lpa.allocation_factor) / pm.current_valuation
        ELSE NULL
    END AS loan_to_value_ratio,
    
    CASE 
        WHEN (lpa.loan_balance * lpa.allocation_factor) > 0 
        THEN pm.current_noi / (lpa.loan_balance * lpa.allocation_factor)
        ELSE NULL
    END AS debt_yield,
    
    -- Per square foot metrics
    pm.current_valuation / NULLIF(pm.square_feet, 0) AS value_per_sq_ft,
    pm.current_revenue / NULLIF(pm.square_feet, 0) AS revenue_per_sq_ft,
    pm.current_noi / NULLIF(pm.square_feet, 0) AS noi_per_sq_ft,
    
    -- Per unit metrics
    pm.current_valuation / NULLIF(pm.unit_count, 0) AS value_per_unit,
    pm.current_revenue / NULLIF(pm.unit_count, 0) AS revenue_per_unit,
    pm.current_noi / NULLIF(pm.unit_count, 0) AS noi_per_unit,
    
    -- Financial ratios
    pm.current_expenses / NULLIF(pm.current_revenue, 0) AS expense_ratio,
    (pm.current_valuation / NULLIF(pm.securitization_valuation, 0)) - 1 AS valuation_change_pct,
    pm.current_occupancy_pct - pm.securitization_occupancy_pct AS occupancy_change_pct,
    
    -- Trend metrics
    pt.valuation_3m_change,
    pt.occupancy_3m_change,
    pt.noi_3m_change,
    pt.valuation_12m_change,
    pt.occupancy_12m_change,
    pt.noi_12m_change,
    
    -- Type benchmarks
    tb.type_avg_occupancy_pct,
    tb.type_avg_value_per_sq_ft,
    tb.type_avg_revenue_per_sq_ft,
    tb.type_avg_noi_per_sq_ft,
    tb.type_avg_expense_ratio,
    tb.type_avg_dscr,
    
    -- Performance categorization
    CASE
        WHEN pm.current_occupancy_pct > tb.type_avg_occupancy_pct THEN 'Above Average'
        WHEN pm.current_occupancy_pct < tb.type_avg_occupancy_pct THEN 'Below Average'
        ELSE 'Average'
    END AS occupancy_vs_type,
    
    CASE
        WHEN pm.current_dscr > tb.type_avg_dscr THEN 'Above Average'
        WHEN pm.current_dscr < tb.type_avg_dscr THEN 'Below Average'
        ELSE 'Average'
    END AS dscr_vs_type,
    
    CASE
        WHEN (pm.current_noi / NULLIF(pm.square_feet, 0)) > tb.type_avg_noi_per_sq_ft THEN 'Above Average'
        WHEN (pm.current_noi / NULLIF(pm.square_feet, 0)) < tb.type_avg_noi_per_sq_ft THEN 'Below Average'
        ELSE 'Average'
    END AS noi_vs_type,
    
    -- Risk categorization
    CASE
        WHEN pm.current_dscr >= 1.5 AND pm.current_occupancy_pct >= 0.9 THEN 'Strong Performer'
        WHEN pm.current_dscr >= 1.2 AND pm.current_occupancy_pct >= 0.8 THEN 'Stable Performer'
        WHEN pm.current_dscr < 1.2 OR pm.current_occupancy_pct < 0.8 THEN 'Underperformer'
        WHEN pm.current_dscr < 1.0 OR pm.current_occupancy_pct < 0.7 THEN 'Distressed'
        ELSE 'Moderate Performer'
    END AS performance_category,
    
    -- Trend classifications
    CASE
        WHEN pt.occupancy_3m_change > 0.02 THEN 'Improving'
        WHEN pt.occupancy_3m_change < -0.02 THEN 'Declining'
        ELSE 'Stable'
    END AS occupancy_trend,
    
    CASE
        WHEN pt.noi_3m_change > 0 THEN 'Improving'
        WHEN pt.noi_3m_change < 0 THEN 'Declining'
        ELSE 'Stable'
    END AS noi_trend,
    
    CURRENT_TIMESTAMP AS loaded_at

FROM property_metrics pm
LEFT JOIN loan_property_allocation lpa 
    ON pm.property_id = lpa.property_id 
    AND pm.reporting_date = lpa.reporting_date
LEFT JOIN property_trends pt 
    ON pm.property_id = pt.property_id 
    AND pm.reporting_date = pt.reporting_date
LEFT JOIN type_benchmarks tb 
    ON pm.property_type_code = tb.property_type_code 
    AND pm.reporting_date = tb.reporting_date 