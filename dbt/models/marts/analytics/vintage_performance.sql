{{
    config(
        materialized='incremental',
        partition_by={
            'field': 'reporting_date',
            'data_type': 'date',
            'granularity': 'month'
        },
        post_hook = [
            "CREATE INDEX IF NOT EXISTS idx_{{ this.name }}_vintage_date ON {{ this }} (vintage_year, reporting_date)",
            "CREATE INDEX IF NOT EXISTS idx_{{ this.name }}_date ON {{ this }} (reporting_date)",
            "ANALYZE {{ this }}"
        ],
        unique_key=['vintage_year', 'reporting_date']
    )
}}

WITH loan_data AS (
    SELECT
        lp.loan_id,
        lp.reporting_date,
        lp.trust_id,
        lp.current_bal,
        lp.delinquency_status,
        lp.days_past_due,
        lp.is_modified,
        CASE 
            WHEN lp.delinquency_status != '0' THEN 1
            ELSE 0
        END AS is_delinquent,
        CASE 
            WHEN lp.days_past_due >= 90 THEN 1
            ELSE 0
        END AS is_90plus_delinquent,
        -- Use the actual origination date from the source data instead of a proxy calculation
        lp.origination_date,
        -- Extract year from the origination date
        EXTRACT(YEAR FROM lp.origination_date) AS vintage_year,
        -- Fallback to trust year if needed
        SUBSTRING(lp.trust_id FROM 'CMBS([0-9]{4})') AS trust_year
    FROM {{ ref('fct_loan_monthly_performance') }} lp
    WHERE lp.reporting_date IS NOT NULL
    {% if is_incremental() %}
    AND lp.reporting_date >= (SELECT max(reporting_date) FROM {{ this }})
    {% endif %}
),

-- Determine vintage year using best available data
loan_vintages AS (
    SELECT
        loan_id,
        reporting_date,
        trust_id,
        current_bal,
        delinquency_status,
        days_past_due,
        is_modified,
        is_delinquent,
        is_90plus_delinquent,
        origination_date,
        vintage_year
    FROM loan_data
),

-- For each reporting date and vintage year, calculate summary metrics
vintage_summary AS (
    SELECT
        reporting_date,
        vintage_year,
        COUNT(DISTINCT loan_id) AS loan_count,
        SUM(current_bal) AS total_balance,
        SUM(CASE WHEN is_delinquent = 1 THEN current_bal ELSE 0 END) AS delinquent_balance,
        SUM(CASE WHEN is_90plus_delinquent = 1 THEN current_bal ELSE 0 END) AS severe_delinquent_balance,
        SUM(CASE WHEN is_modified = TRUE THEN current_bal ELSE 0 END) AS modified_balance,
        AVG(days_past_due) AS avg_days_past_due,
        -- Calculate percentages
        SUM(CASE WHEN is_delinquent = 1 THEN current_bal ELSE 0 END) / NULLIF(SUM(current_bal), 0) AS delinquency_rate,
        SUM(CASE WHEN is_90plus_delinquent = 1 THEN current_bal ELSE 0 END) / NULLIF(SUM(current_bal), 0) AS severe_delinquency_rate,
        SUM(CASE WHEN is_modified = TRUE THEN current_bal ELSE 0 END) / NULLIF(SUM(current_bal), 0) AS modification_rate,
        AVG(current_bal) AS avg_loan_balance
    FROM loan_vintages
    WHERE vintage_year >= 2000 AND vintage_year <= EXTRACT(YEAR FROM CURRENT_DATE)
    GROUP BY reporting_date, vintage_year
),

-- Calculate vintage age (how many months since origination)
vintage_age AS (
    SELECT
        vs.*,
        (EXTRACT(YEAR FROM reporting_date) - vintage_year) * 12 + 
        (EXTRACT(MONTH FROM reporting_date) - 1) AS months_since_vintage
    FROM vintage_summary vs
),

-- Calculate performance metrics at same age across vintages
-- This allows comparing 2018 vintage at 24 months vs 2019 vintage at 24 months
vintage_cohort_analysis AS (
    SELECT
        va.*,
        -- Get average delinquency rate for all vintages at this same age
        AVG(va.delinquency_rate) OVER (
            PARTITION BY va.months_since_vintage
        ) AS avg_delinquency_at_age,
        -- Get max and min delinquency rates for all vintages at this same age
        MAX(va.delinquency_rate) OVER (
            PARTITION BY va.months_since_vintage
        ) AS max_delinquency_at_age,
        MIN(va.delinquency_rate) OVER (
            PARTITION BY va.months_since_vintage
        ) AS min_delinquency_at_age,
        -- Calculate performance relative to average at same age
        va.delinquency_rate - AVG(va.delinquency_rate) OVER (
            PARTITION BY va.months_since_vintage
        ) AS delinquency_vs_avg_at_age,
        -- Calculate percentile ranking within age group
        PERCENT_RANK() OVER (
            PARTITION BY va.months_since_vintage
            ORDER BY va.delinquency_rate
        ) AS delinquency_percentile_at_age
    FROM vintage_age va
),

-- Calculate vintage trend metrics
vintage_trends AS (
    SELECT
        vca.*,
        -- Calculate 3-month trend for this vintage
        vca.delinquency_rate - LAG(vca.delinquency_rate, 3) OVER (
            PARTITION BY vca.vintage_year
            ORDER BY vca.reporting_date
        ) AS delinquency_3m_change,
        -- Calculate 6-month trend for this vintage
        vca.delinquency_rate - LAG(vca.delinquency_rate, 6) OVER (
            PARTITION BY vca.vintage_year
            ORDER BY vca.reporting_date
        ) AS delinquency_6m_change,
        -- Calculate 12-month trend for this vintage
        vca.delinquency_rate - LAG(vca.delinquency_rate, 12) OVER (
            PARTITION BY vca.vintage_year
            ORDER BY vca.reporting_date
        ) AS delinquency_12m_change,
        -- Track if this is the most recent reporting date for the analysis
        reporting_date = MAX(reporting_date) OVER () AS is_latest_reporting_date
    FROM vintage_cohort_analysis vca
)

-- Final output
SELECT
    reporting_date,
    vintage_year,
    months_since_vintage AS vintage_age_months,
    loan_count,
    total_balance,
    delinquent_balance,
    severe_delinquent_balance,
    modified_balance,
    avg_days_past_due,
    delinquency_rate,
    severe_delinquency_rate,
    modification_rate,
    avg_loan_balance,
    -- Comparative metrics
    avg_delinquency_at_age,
    max_delinquency_at_age,
    min_delinquency_at_age,
    delinquency_vs_avg_at_age,
    delinquency_percentile_at_age,
    -- Trend metrics 
    delinquency_3m_change,
    delinquency_6m_change,
    delinquency_12m_change,
    -- Categorize vintage performance
    CASE
        WHEN delinquency_percentile_at_age < 0.25 THEN 'Top Performer'
        WHEN delinquency_percentile_at_age < 0.5 THEN 'Above Average'
        WHEN delinquency_percentile_at_age < 0.75 THEN 'Below Average'
        ELSE 'Underperformer'
    END AS vintage_performance_category,
    -- Trend assessment
    CASE
        WHEN delinquency_3m_change IS NULL THEN 'Insufficient Data'
        WHEN delinquency_3m_change < -0.01 THEN 'Improving'
        WHEN delinquency_3m_change > 0.01 THEN 'Deteriorating'
        ELSE 'Stable'
    END AS short_term_trend,
    -- Add current timestamp
    CURRENT_TIMESTAMP AS loaded_at
FROM vintage_trends
ORDER BY reporting_date, vintage_year 