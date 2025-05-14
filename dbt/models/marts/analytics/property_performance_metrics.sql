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
            "CREATE INDEX IF NOT EXISTS idx_{{ this.name }}_property_date ON {{ this }} (property_id, reporting_date)",
            "CREATE INDEX IF NOT EXISTS idx_{{ this.name }}_property_type ON {{ this }} (property_type_code, reporting_date)",
            "CREATE INDEX IF NOT EXISTS idx_{{ this.name }}_geo ON {{ this }} (state_code, reporting_date)",
            "ANALYZE {{ this }}"
        ]
    )
}}

/* 
 * Analytics model for property metrics, joining through the snowflake schema geography dimensions.
 * This model denormalizes property information with geographic attributes for analysis.
 * Uses Type 2 SCD for historical property attributes and proper geographic hierarchy.
 */
WITH property_data AS (
    SELECT
        -- Property metrics
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
        
        -- Property attributes from Type 2 SCD
        dp.property_type_code,
        dp.square_feet,
        dp.unit_count,
        
        -- Geography from snowflake schema
        a.property_address,
        z.zip_code,
        c.city_name,
        s.state_code,
        s.state_name,
        s.region_name,
        s.division_name,
        
        -- Loan data
        lp.current_bal AS loan_balance,
        lp.current_intr_rate,
        lp.origination_date,
        
        -- Calculated metrics
        CASE 
            WHEN pm.current_valuation > 0 THEN lp.current_bal / pm.current_valuation
            ELSE NULL
        END AS loan_to_value_ratio,
        
        CASE 
            WHEN lp.current_bal > 0 THEN pm.current_noi / lp.current_bal
            ELSE NULL
        END AS debt_yield
    FROM {{ ref('fct_property_metrics') }} pm
    -- Join to property dimension with Type 2 SCD
    JOIN {{ ref('dim_property') }} dp 
        ON pm.property_id = dp.property_id
        AND pm.reporting_date >= dp.effective_date 
        AND pm.reporting_date < dp.end_date
    -- Join to geographic snowflake schema
    LEFT JOIN {{ ref('dim_geo_address') }} a ON dp.geo_id = a.address_id
    LEFT JOIN {{ ref('dim_geo_zip') }} z ON a.zip_id = z.zip_id
    LEFT JOIN {{ ref('dim_geo_city') }} c ON z.city_id = c.city_id
    LEFT JOIN {{ ref('dim_geo_state') }} s ON c.state_id = s.state_id
    -- Join to loan performance
    LEFT JOIN {{ ref('fct_loan_monthly_performance') }} lp 
        ON pm.property_id = lp.loan_id
        AND pm.reporting_date = lp.reporting_date
    WHERE pm.reporting_date IS NOT NULL
    {% if is_incremental() %}
    AND pm.reporting_date >= (SELECT max(reporting_date) FROM {{ this }})
    {% endif %}
),

property_metrics AS (
    SELECT
        pd.*,
        -- Value metrics
        pd.current_valuation / NULLIF(pd.securitization_valuation, 0) - 1 AS valuation_change_pct,
        pd.current_occupancy_pct - pd.securitization_occupancy_pct AS occupancy_change_pct,
        
        -- Per square foot metrics
        pd.current_valuation / NULLIF(pd.square_feet, 0) AS value_per_sq_ft,
        pd.current_revenue / NULLIF(pd.square_feet, 0) AS revenue_per_sq_ft,
        pd.current_noi / NULLIF(pd.square_feet, 0) AS noi_per_sq_ft,
        
        -- Per unit metrics
        pd.current_valuation / NULLIF(pd.unit_count, 0) AS value_per_unit,
        pd.current_revenue / NULLIF(pd.unit_count, 0) AS revenue_per_unit,
        pd.current_noi / NULLIF(pd.unit_count, 0) AS noi_per_unit,
        
        -- Expense ratio
        pd.current_expenses / NULLIF(pd.current_revenue, 0) AS expense_ratio
    FROM property_data pd
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
        AVG(value_per_sq_ft) AS type_avg_value_per_sq_ft,
        AVG(revenue_per_sq_ft) AS type_avg_revenue_per_sq_ft,
        AVG(noi_per_sq_ft) AS type_avg_noi_per_sq_ft,
        AVG(expense_ratio) AS type_avg_expense_ratio,
        AVG(debt_yield) AS type_avg_debt_yield,
        AVG(current_dscr) AS type_avg_dscr
    FROM property_metrics
    WHERE property_type_code IS NOT NULL
    GROUP BY reporting_date, property_type_code
)

SELECT
    -- Property identifiers
    pm.property_id,
    pm.reporting_date,
    
    -- Core property metrics
    pm.current_valuation,
    pm.securitization_valuation,
    pm.current_occupancy_pct,
    pm.securitization_occupancy_pct,
    pm.current_revenue,
    pm.current_expenses,
    pm.current_noi,
    pm.current_dscr,
    pm.property_type_code,
    
    -- Geographic attributes (denormalized for analytics)
    pm.property_address,
    pm.zip_code,
    pm.city_name,
    pm.state_code,
    pm.state_name,
    pm.region_name,
    pm.division_name,
    
    -- Loan metrics
    pm.loan_balance,
    pm.current_intr_rate,
    pm.origination_date,
    pm.loan_to_value_ratio,
    pm.debt_yield,
    
    -- Property attributes
    pm.square_feet,
    pm.unit_count,
    
    -- Derived metrics
    pm.value_per_sq_ft,
    pm.revenue_per_sq_ft,
    pm.noi_per_sq_ft,
    pm.value_per_unit,
    pm.revenue_per_unit,
    pm.noi_per_unit,
    pm.expense_ratio,
    pm.valuation_change_pct,
    pm.occupancy_change_pct,
    
    -- Trend metrics
    pt.valuation_3m_change,
    pt.occupancy_3m_change,
    pt.noi_3m_change,
    pt.valuation_12m_change,
    pt.occupancy_12m_change,
    pt.noi_12m_change,
    
    -- Benchmark comparisons
    tb.type_avg_occupancy_pct,
    tb.type_avg_value_per_sq_ft,
    tb.type_avg_revenue_per_sq_ft,
    tb.type_avg_noi_per_sq_ft,
    tb.type_avg_expense_ratio,
    tb.type_avg_debt_yield,
    tb.type_avg_dscr,
    
    -- Comparative metrics
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
        WHEN pm.noi_per_sq_ft > tb.type_avg_noi_per_sq_ft THEN 'Above Average'
        WHEN pm.noi_per_sq_ft < tb.type_avg_noi_per_sq_ft THEN 'Below Average'
        ELSE 'Average'
    END AS noi_vs_type,
    
    -- Performance categories
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
    
    -- Risk score (0-10, higher is riskier)
    (
        -- DSCR component (0-3 points)
        CASE
            WHEN pm.current_dscr >= 1.5 THEN 0
            WHEN pm.current_dscr >= 1.2 THEN 1
            WHEN pm.current_dscr >= 1.0 THEN 2
            ELSE 3
        END +
        
        -- Occupancy component (0-2 points)
        CASE
            WHEN pm.current_occupancy_pct >= 0.9 THEN 0
            WHEN pm.current_occupancy_pct >= 0.8 THEN 1
            WHEN pm.current_occupancy_pct >= 0.7 THEN 1.5
            ELSE 2
        END +
        
        -- LTV component (0-2 points)
        CASE
            WHEN pm.loan_to_value_ratio IS NULL THEN 1
            WHEN pm.loan_to_value_ratio <= 0.6 THEN 0
            WHEN pm.loan_to_value_ratio <= 0.7 THEN 0.5
            WHEN pm.loan_to_value_ratio <= 0.8 THEN 1
            ELSE 2
        END +
        
        -- Debt yield component (0-1.5 points)
        CASE
            WHEN pm.debt_yield IS NULL THEN 0.75
            WHEN pm.debt_yield >= 0.10 THEN 0
            WHEN pm.debt_yield >= 0.08 THEN 0.5
            WHEN pm.debt_yield >= 0.06 THEN 1
            ELSE 1.5
        END +
        
        -- Trend component (0-1.5 points)
        CASE
            WHEN pt.occupancy_3m_change < -0.03 OR pt.noi_3m_change < 0 THEN 1.5
            WHEN pt.occupancy_3m_change < -0.02 OR pt.noi_3m_change < 0 THEN 1
            WHEN pt.occupancy_3m_change < -0.01 THEN 0.5
            ELSE 0
        END
    ) AS property_risk_score,
    
    CURRENT_TIMESTAMP AS loaded_at
FROM property_metrics pm
LEFT JOIN property_trends pt 
    ON pm.property_id = pt.property_id 
    AND pm.reporting_date = pt.reporting_date
LEFT JOIN type_benchmarks tb
    ON pm.reporting_date = tb.reporting_date
    AND pm.property_type_code = tb.property_type_code 