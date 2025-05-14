{{
  config(
    materialized = 'incremental',
    unique_key = ['loan_id', 'reporting_date', 'trust_id'],
    partition_by = {
      "field": "reporting_date",
      "data_type": "date",
      "granularity": "month"
    },
    post_hook = [
      "CREATE INDEX IF NOT EXISTS idx_{{ this.name }}_loan_date ON {{ this }} (loan_id, reporting_date)",
      "CREATE INDEX IF NOT EXISTS idx_{{ this.name }}_trust_date ON {{ this }} (trust_id, reporting_date)",
      "ANALYZE {{ this }}"
    ]
  )
}}

WITH loan_data AS (
    SELECT DISTINCT ON (asset_num, reporting_period_end_date)
        -- Identifiers
        asset_num AS loan_id,
        reporting_period_end_date AS reporting_date,
        trust AS trust_name,
        company AS issuer,
        
        -- Loan characteristics
        original_loan_amt AS orig_bal,
        report_period_end_scheduled_loan_bal_amt AS current_bal,
        report_period_begin_schedule_loan_bal_amt AS begin_bal,
        report_period_intr_rate_pct AS current_intr_rate,
        payment_frequency_code AS payment_frequency,
        {{ fill_scd_value('maturity_date', ['asset_num']) }} AS maturity_date,
        origination_date,
        
        -- Status and payments
        CASE
            WHEN payment_status_loan_code IN ('A', 'B') THEN '0'
            ELSE COALESCE(payment_status_loan_code, '0')
        END AS delinquency_status,
        modified_indicator AS is_modified,
        report_period_modification_indicator AS modified_this_period,
        total_scheduled_prin_intr_due_amt AS pmt_due,
        scheduled_prin_amt + scheduled_intr_amt AS pmt_received
    FROM {{ source('cmbs', 'loans') }}
    {% if is_incremental() %}
    WHERE reporting_period_end_date > (SELECT MAX(reporting_date) FROM {{ this }})
    {% endif %}
),

-- Simple handling of A/B notes - just COALESCE values
loan_data_with_split_notes AS (
    SELECT 
        l1.loan_id,
        l1.reporting_date,
        l1.trust_name,
        l1.issuer,
        COALESCE(l1.orig_bal, l2.orig_bal) AS orig_bal,
        COALESCE(l1.current_bal, l2.current_bal) AS current_bal,
        COALESCE(l1.begin_bal, l2.begin_bal) AS begin_bal,
        COALESCE(l1.current_intr_rate, l2.current_intr_rate) AS current_intr_rate,
        COALESCE(l1.payment_frequency, l2.payment_frequency) AS payment_frequency,
        COALESCE(l1.maturity_date, l2.maturity_date) AS maturity_date,
        COALESCE(l1.origination_date, l2.origination_date) AS origination_date,
        COALESCE(l1.delinquency_status, l2.delinquency_status) AS delinquency_status,
        COALESCE(l1.is_modified, l2.is_modified) AS is_modified,
        COALESCE(l1.modified_this_period, l2.modified_this_period) AS modified_this_period,
        COALESCE(l1.pmt_due, l2.pmt_due) AS pmt_due,
        COALESCE(l1.pmt_received, l2.pmt_received) AS pmt_received
    FROM loan_data l1
    LEFT JOIN loan_data l2
        ON l2.loan_id = l1.loan_id || 'A'
        AND l2.reporting_date = l1.reporting_date
)

SELECT
    -- Generate surrogate key
    {{ dbt_utils.generate_surrogate_key(['ld.loan_id', 'ld.reporting_date', 'dt.trust_id']) }} AS loan_performance_id,
    
    -- Base identifiers
    ld.loan_id,
    ld.reporting_date,
    dt.trust_id,
    
    -- Loan balances
    ld.current_bal,
    ld.begin_bal,
    ld.orig_bal,
    
    -- Status flags
    ld.delinquency_status,
    ld.is_modified,
    ld.modified_this_period,
    ld.maturity_date < ld.reporting_date AS is_past_maturity,
    
    -- Calculate days past due
    CASE 
        WHEN ld.delinquency_status = '0' THEN 0
        WHEN ld.delinquency_status = '1' THEN 30
        WHEN ld.delinquency_status = '2' THEN 60
        WHEN ld.delinquency_status = '3' THEN 90
        WHEN ld.delinquency_status = '4' THEN 120
        WHEN ld.delinquency_status = '5' THEN 150
        WHEN ld.delinquency_status = '6' THEN 180
        ELSE 0
    END AS days_past_due,
    
    -- Loan status categorization
    CASE
        WHEN ld.delinquency_status = '0' THEN 'Current'
        WHEN ld.delinquency_status IN ('1', '2') THEN 'Delinquent < 90 Days'
        WHEN ld.delinquency_status IN ('3', '4', '5', '6') THEN 'Delinquent 90+ Days'
        ELSE 'Other'
    END AS loan_status,
    
    -- Payment metrics
    ld.pmt_due,
    ld.pmt_received,
    ld.pmt_due - ld.pmt_received AS pmt_shortfall,
    
    -- Loan characteristics
    ld.current_intr_rate,
    ld.payment_frequency,
    ld.maturity_date,
    ld.origination_date,
    ld.issuer,
    
    -- Derived metrics
    CASE 
        WHEN ld.orig_bal > 0 
        THEN ld.current_bal / ld.orig_bal 
        ELSE NULL 
    END AS remaining_bal_pct,
    
    CASE 
        WHEN ld.maturity_date IS NOT NULL 
        THEN DATE_PART('month', AGE(ld.maturity_date, ld.reporting_date)) 
        ELSE NULL 
    END AS months_to_maturity,
    
    CASE 
        WHEN ld.origination_date IS NOT NULL 
        THEN DATE_PART('month', AGE(ld.reporting_date, ld.origination_date)) 
        ELSE NULL 
    END AS months_since_origination,
    
    -- Delinquent amount calculation
    CASE 
        WHEN ld.delinquency_status != '0' 
        THEN ld.pmt_due - ld.pmt_received
        ELSE 0 
    END AS delinquent_amt,
    
    CURRENT_TIMESTAMP AS loaded_at
FROM loan_data_with_split_notes ld
JOIN {{ ref('dim_trust') }} dt ON ld.trust_name = dt.trust_name 