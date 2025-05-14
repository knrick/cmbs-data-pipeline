{{
  config(
    materialized = 'table',
    schema = 'mart',
    indexes = [
      {'columns': ['loan_id']},
      {'columns': ['reporting_date']},
      {'columns': ['origination_date']}
    ]
  )
}}

WITH loan_dates AS (
    SELECT
        loan_id,
        trust_id,
        MIN(reporting_date) as first_reporting_date,
        MAX(reporting_date) as latest_reporting_date,
        MIN(origination_date) as origination_date
    FROM {{ ref('fct_loan_monthly_performance') }}
    WHERE NOT is_past_maturity
    GROUP BY 1, 2
),

loan_age_metrics AS (
    SELECT
        perf.loan_id,
        perf.trust_id,
        perf.maturity_date,
        perf.reporting_date,
        perf.current_bal,
        perf.orig_bal,
        perf.origination_date,
        -- Calculate loan age in months
        DATE_PART('month', AGE(perf.reporting_date, perf.origination_date)) + 
        DATE_PART('year', AGE(perf.reporting_date, perf.origination_date)) * 12 + 1 AS loan_age_months,
        -- Use months_to_maturity from loan performance
        perf.months_to_maturity,
        -- Original term
        DATE_PART('month', AGE(perf.maturity_date, perf.origination_date)) + 
        DATE_PART('year', AGE(perf.maturity_date, perf.origination_date)) * 12 + 1 AS original_term_months,
        -- Principal reduction (using balance difference)
        CASE 
            WHEN LAG(perf.current_bal) OVER (PARTITION BY perf.loan_id ORDER BY perf.reporting_date) > perf.current_bal
            THEN LAG(perf.current_bal) OVER (PARTITION BY perf.loan_id ORDER BY perf.reporting_date) - perf.current_bal
            ELSE 0 
        END as principal_reduction,
        -- Calculate if prepayment occurred using balance reduction
        CASE 
            WHEN LAG(perf.current_bal) OVER (PARTITION BY perf.loan_id ORDER BY perf.reporting_date) > perf.current_bal
            THEN TRUE 
            ELSE FALSE 
        END as is_prepayment
    FROM {{ ref('fct_loan_monthly_performance') }} perf
    JOIN loan_dates ld ON perf.loan_id = ld.loan_id
    WHERE NOT perf.is_past_maturity
),

prepayment_metrics AS (
    SELECT
        loan_id,
        trust_id,
        maturity_date,
        reporting_date,
        loan_age_months,
        months_to_maturity,
        original_term_months,
        origination_date,
        -- Calculate PSA (Public Securities Association) prepayment speed
        CASE 
            WHEN loan_age_months <= 30 
            THEN (principal_reduction / NULLIF(current_bal, 0)) * 100 * (30 / loan_age_months)
            ELSE (principal_reduction / NULLIF(current_bal, 0)) * 100
        END as psa_speed,
        -- Calculate CPR (Conditional Prepayment Rate)
        (principal_reduction / NULLIF(current_bal, 0)) * 100 as cpr,
        -- Calculate SMM (Single Monthly Mortality)
        (principal_reduction / NULLIF(LAG(current_bal) OVER (PARTITION BY loan_id ORDER BY reporting_date), 0)) * 100 as smm,
        -- Prepayment penalties if applicable
        is_prepayment,
        -- Balance metrics
        current_bal,
        orig_bal,
        (current_bal / NULLIF(orig_bal, 0)) * 100 as percent_remaining_balance
    FROM loan_age_metrics
)

SELECT
    pm.*,
    -- Add vintage grouping
    DATE_TRUNC('year', origination_date) as origination_vintage_year,
    DATE_TRUNC('quarter', origination_date) as origination_vintage_quarter,
    -- Add maturity grouping
    DATE_TRUNC('year', reporting_date + (months_to_maturity * INTERVAL '1 month')) as maturity_vintage_year,
    -- Age buckets for analysis
    CASE 
        WHEN loan_age_months <= 12 THEN '0-1 Year'
        WHEN loan_age_months <= 24 THEN '1-2 Years'
        WHEN loan_age_months <= 36 THEN '2-3 Years'
        WHEN loan_age_months <= 60 THEN '3-5 Years'
        WHEN loan_age_months <= 84 THEN '5-7 Years'
        WHEN loan_age_months <= 120 THEN '7-10 Years'
        ELSE 'Over 10 Years'
    END as loan_age_bucket,
    -- Remaining term buckets
    CASE 
        WHEN months_to_maturity <= 12 THEN 'Under 1 Year'
        WHEN months_to_maturity <= 36 THEN '1-3 Years'
        WHEN months_to_maturity <= 60 THEN '3-5 Years'
        WHEN months_to_maturity <= 120 THEN '5-10 Years'
        ELSE 'Over 10 Years'
    END as remaining_term_bucket
FROM prepayment_metrics pm 