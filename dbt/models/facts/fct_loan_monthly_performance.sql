{{
  config(
    materialized = 'incremental',
    unique_key = ['loan_id', 'reporting_date', 'trust_id', 'source_file'],
    partition_by = {
      "field": "reporting_date",
      "data_type": "date",
      "granularity": "month"
    },
    post_hook = [
      "CREATE INDEX IF NOT EXISTS idx_{{ this.name }}_loan_date ON {{ this }} (loan_id, reporting_date)",
      "CREATE INDEX IF NOT EXISTS idx_{{ this.name }}_trust_date ON {{ this }} (trust_id, reporting_date)",
      "CREATE INDEX IF NOT EXISTS idx_{{ this.name }}_delinquency ON {{ this }} (delinquency_status, reporting_date)",
      "ANALYZE {{ this }}"
    ]
  )
}}

WITH loan_monthly_data AS (
    SELECT
        l.asset_num AS loan_id,
        l.reporting_period_end_date AS reporting_date,
        l.trust AS trust_id,
        
        -- Loan status metrics
        COALESCE(NULLIF(l.payment_status_loan_code, ''), '0') AS delinquency_status,
        l.modified_indicator AS is_modified,
        l.report_period_modification_indicator AS modified_this_period,
        l.maturity_date < l.reporting_period_end_date AS is_past_maturity,
        
        -- Balance metrics
        l.report_period_end_scheduled_loan_bal_amt AS current_bal,
        l.report_period_begin_schedule_loan_bal_amt AS begin_bal,
        l.original_loan_amt AS orig_bal,
        
        -- Calculated balance metrics
        CASE 
            WHEN l.original_loan_amt > 0 
            THEN l.report_period_end_scheduled_loan_bal_amt / l.original_loan_amt 
            ELSE NULL 
        END AS remaining_bal_pct,
        
        CASE 
            WHEN l.report_period_begin_schedule_loan_bal_amt > 0 
            THEN (l.report_period_end_scheduled_loan_bal_amt - l.report_period_begin_schedule_loan_bal_amt) 
                / l.report_period_begin_schedule_loan_bal_amt 
            ELSE NULL 
        END AS bal_change_pct,
        
        -- Payment metrics
        l.total_scheduled_prin_intr_due_amt AS pmt_due,
        l.scheduled_prin_amt + l.scheduled_intr_amt AS pmt_received,
        l.total_scheduled_prin_intr_due_amt - 
          (l.scheduled_prin_amt + l.scheduled_intr_amt) AS pmt_shortfall,
          
        -- Interest metrics  
        l.report_period_intr_rate_pct AS current_intr_rate,
        
        -- Dates
        l.maturity_date,
        
        -- Metadata
        l.company AS issuer,
        l.trust AS trust_name,
        l.source_file
    FROM {{ source('cmbs', 'loans') }} l
    WHERE l.reporting_period_end_date IS NOT NULL
    
    {% if is_incremental() %}
      -- Only process new data since last run
      AND l.reporting_period_end_date > (
        SELECT COALESCE(MAX(reporting_date), '2000-01-01'::date) FROM {{ this }}
      )
    {% endif %}
)

SELECT
    -- Generate surrogate key using dbt_utils
    {{ dbt_utils.generate_surrogate_key(['loan_id', 'reporting_date', 'trust_id', 'source_file']) }} AS performance_id,
    
    -- Foreign keys for dimension tables
    loan_id,
    reporting_date,
    trust_id,
    
    -- Loan status metrics
    delinquency_status,
    is_modified,
    modified_this_period,
    is_past_maturity,
    
    -- Calculate DPD (Days Past Due) based on delinquency status
    CASE 
        WHEN delinquency_status = '0' THEN 0
        WHEN delinquency_status = '1' THEN 30
        WHEN delinquency_status = '2' THEN 60
        WHEN delinquency_status = '3' THEN 90
        WHEN delinquency_status = '4' THEN 120
        WHEN delinquency_status = '5' THEN 150
        WHEN delinquency_status = '6' THEN 180
        ELSE 0
    END AS days_past_due,
    
    -- Status flags
    CASE 
        WHEN delinquency_status = '0' THEN 'Current'
        WHEN delinquency_status IN ('1', '2') THEN 'Delinquent < 90 Days'
        WHEN delinquency_status IN ('3', '4', '5', '6') THEN 'Delinquent 90+ Days'
        ELSE 'Other'
    END AS loan_status,
    
    -- Balance metrics
    current_bal,
    begin_bal,
    orig_bal,
    remaining_bal_pct,
    bal_change_pct,
    
    -- Payment metrics
    pmt_due,
    pmt_received,
    pmt_shortfall,
    
    -- Interest metrics
    current_intr_rate,
    
    -- Important dates
    maturity_date,
    
    -- Source file for tracking
    source_file,
    
    -- Delinquent amount calculation
    CASE 
        WHEN delinquency_status != '0' THEN pmt_shortfall
        ELSE 0
    END AS delinquent_amt,
    
    -- Timestamp
    CURRENT_TIMESTAMP AS loaded_at
FROM loan_monthly_data 