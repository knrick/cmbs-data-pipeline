{{
    config(
        materialized='table',
        partition_by={
            'field': 'reporting_date',
            'data_type': 'date',
            'granularity': 'month'
        },
        post_hook = [
            "CREATE INDEX IF NOT EXISTS idx_{{ this.name }}_trust_date ON {{ this }} (trust_id, reporting_date)",
            "CREATE INDEX IF NOT EXISTS idx_{{ this.name }}_property_type ON {{ this }} (property_type_code, reporting_date)",
            "ANALYZE {{ this }}"
        ]
    )
}}

/*
 * Loan Risk Metrics Model
 * 
 * This model analyzes loan risk factors by joining loan data with property metrics 
 * and properly navigating the snowflake schema to access geographic information.
 */
WITH loan_and_property_data AS (
    SELECT
        -- Loan metrics
        lp.loan_id,
        lp.reporting_date,
        lp.trust_id,
        lp.current_bal,
        lp.days_past_due,
        lp.delinquency_status,
        lp.current_intr_rate,
        lp.begin_bal,
        
        -- Property metrics
        dp.property_id,
        dp.property_type_code,
        pm.current_dscr,
        pm.current_occupancy_pct,
        pm.valuation_change_pct,
        
        -- Geography via snowflake schema
        s.state_code AS property_state,
        
        -- Calculate LTV (loan-to-value ratio) where we have property valuation data
        CASE 
            WHEN pm.current_valuation > 0 THEN lp.current_bal / pm.current_valuation
            ELSE NULL
        END AS loan_to_value_ratio,
        
        -- Calculate month-over-month changes
        lp.current_bal - lp.begin_bal AS loan_amount_change
    FROM {{ ref('fct_loan_monthly_performance') }} lp
    -- Join to property dimension using SCD Type 2 date range
    LEFT JOIN {{ ref('dim_property') }} dp 
        ON lp.loan_id = dp.property_id
        AND lp.reporting_date >= dp.effective_date 
        AND lp.reporting_date < dp.end_date
    LEFT JOIN {{ ref('fct_property_metrics') }} pm 
        ON dp.property_id = pm.property_id 
        AND lp.reporting_date = pm.reporting_date
    -- Geographic snowflake joins
    LEFT JOIN {{ ref('dim_geo_address') }} a ON dp.geo_id = a.address_id
    LEFT JOIN {{ ref('dim_geo_zip') }} z ON a.zip_id = z.zip_id
    LEFT JOIN {{ ref('dim_geo_city') }} c ON z.city_id = c.city_id
    LEFT JOIN {{ ref('dim_geo_state') }} s ON c.state_id = s.state_id
    WHERE NOT lp.is_past_maturity
),

risk_metrics AS (
    SELECT
        loan_id,
        property_id,
        trust_id,
        reporting_date,
        property_type_code,
        property_state,
        -- Current metrics
        current_bal,
        loan_to_value_ratio,
        current_dscr,
        current_occupancy_pct,
        
        -- Risk indicators
        CASE
            WHEN loan_to_value_ratio > 0.8 THEN 'High'
            WHEN loan_to_value_ratio > 0.6 THEN 'Medium'
            ELSE 'Low'
        END AS ltv_risk,
        
        CASE
            WHEN current_dscr < 1.2 THEN 'High'
            WHEN current_dscr < 1.5 THEN 'Medium'
            ELSE 'Low'
        END AS dscr_risk,
        
        CASE
            WHEN current_occupancy_pct < 0.8 THEN 'High'
            WHEN current_occupancy_pct < 0.9 THEN 'Medium'
            ELSE 'Low'
        END AS occupancy_risk,
        
        CASE
            WHEN days_past_due > 60 THEN 'High'
            WHEN days_past_due > 30 THEN 'Medium'
            ELSE 'Low'
        END AS delinquency_risk,
        
        -- Trend analysis
        CASE
            WHEN loan_amount_change > 0 THEN 'Increasing'
            WHEN loan_amount_change < 0 THEN 'Decreasing'
            ELSE 'Stable'
        END AS loan_amount_trend,
        
        CASE
            WHEN valuation_change_pct < -0.05 THEN 'Declining'
            WHEN valuation_change_pct > 0.05 THEN 'Improving'
            ELSE 'Stable'
        END AS valuation_trend,
        
        -- Composite risk score (0-10, higher is riskier)
        (
            -- LTV component (0-4 points)
            CASE
                WHEN loan_to_value_ratio > 0.8 THEN 4
                WHEN loan_to_value_ratio > 0.6 THEN 2
                WHEN loan_to_value_ratio IS NOT NULL THEN 0
                ELSE 2  -- Default medium risk when unknown
            END +
            
            -- DSCR component (0-4 points)
            CASE
                WHEN current_dscr < 1.2 THEN 4
                WHEN current_dscr < 1.5 THEN 2
                WHEN current_dscr IS NOT NULL THEN 0
                ELSE 2  -- Default medium risk when unknown
            END +
            
            -- Occupancy component (0-2 points)
            CASE
                WHEN current_occupancy_pct < 0.8 THEN 2
                WHEN current_occupancy_pct < 0.9 THEN 1
                WHEN current_occupancy_pct IS NOT NULL THEN 0
                ELSE 1  -- Default medium risk when unknown
            END +
            
            -- Delinquency component (0-10 points)
            CASE
                WHEN delinquency_status IN ('3', '4', '5', '6') THEN 10  -- 90+ days = very high risk
                WHEN delinquency_status IN ('1', '2') THEN 6  -- 30-60 days = high risk
                ELSE 0  -- Current
            END
        ) / 2 AS risk_score  -- Normalize to 0-10 scale
    FROM loan_and_property_data
),

SELECT
    trust_id,
    property_type_code,
    property_state,
    reporting_date,
    
    -- Portfolio metrics
    COUNT(*) AS loan_count,
    SUM(current_bal) AS total_loan_amt,
    AVG(loan_to_value_ratio) AS avg_ltv,
    AVG(current_dscr) AS avg_dscr,
    AVG(current_occupancy_pct) AS avg_occupancy,
    
    -- Risk distribution
    SUM(CASE WHEN ltv_risk = 'High' THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0)::FLOAT AS high_ltv_pct,
    SUM(CASE WHEN dscr_risk = 'High' THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0)::FLOAT AS high_dscr_pct,
    SUM(CASE WHEN occupancy_risk = 'High' THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0)::FLOAT AS high_occupancy_risk_pct,
    SUM(CASE WHEN delinquency_risk = 'High' THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0)::FLOAT AS high_delinquency_risk_pct,
    
    -- Average risk score
    AVG(risk_score) AS avg_risk_score,
    
    -- Risk stratification
    SUM(CASE WHEN risk_score >= 8 THEN current_bal ELSE 0 END) AS high_risk_bal,
    SUM(CASE WHEN risk_score >= 4 AND risk_score < 8 THEN current_bal ELSE 0 END) AS medium_risk_bal,
    SUM(CASE WHEN risk_score < 4 THEN current_bal ELSE 0 END) AS low_risk_bal,
    
    -- Calculate risk ratios
    SUM(CASE WHEN risk_score >= 8 THEN current_bal ELSE 0 END) / NULLIF(SUM(current_bal), 0) AS high_risk_ratio,
    SUM(CASE WHEN risk_score >= 4 AND risk_score < 8 THEN current_bal ELSE 0 END) / NULLIF(SUM(current_bal), 0) AS medium_risk_ratio,
    SUM(CASE WHEN risk_score < 4 THEN current_bal ELSE 0 END) / NULLIF(SUM(current_bal), 0) AS low_risk_ratio
FROM risk_metrics
GROUP BY trust_id, property_type_code, property_state, reporting_date