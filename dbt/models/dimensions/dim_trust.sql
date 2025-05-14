{{
  config(
    materialized = 'table',
    post_hook = [
      "CREATE INDEX IF NOT EXISTS idx_{{ this.name }}_trust_id ON {{ this }} (trust_id)",
      "CREATE INDEX IF NOT EXISTS idx_{{ this.name }}_trust_name ON {{ this }} (trust_name)",
      "ANALYZE {{ this }}"
    ]
  )
}}

WITH trust_metadata AS (
    SELECT 
        trust,
        company AS issuer,
        MIN(reporting_period_end_date) AS first_reporting_date,
        MAX(reporting_period_end_date) AS latest_reporting_date,
        COUNT(DISTINCT asset_num) AS loan_count,
        SUM(original_loan_amt) AS total_orig_bal
    FROM {{ source('cmbs', 'loans') }}
    GROUP BY trust, company
),

-- Calculate latest month's summary
latest_metrics AS (
    SELECT
        trust,
        reporting_period_end_date,
        COUNT(DISTINCT asset_num) AS current_loan_count,
        SUM(CASE WHEN payment_status_loan_code IN ('1', '2', '3', '4', '5') THEN 1 ELSE 0 END) AS delinquent_loans,
        SUM(report_period_end_scheduled_loan_bal_amt) AS current_bal,
        ROW_NUMBER() OVER (PARTITION BY trust ORDER BY reporting_period_end_date DESC) AS recency_rank
    FROM {{ source('cmbs', 'loans') }}
    GROUP BY trust, reporting_period_end_date
)

SELECT
    -- Primary key (surrogate key)
    {{ dbt_utils.generate_surrogate_key(['tm.trust']) }} AS trust_id,
    
    -- Trust attributes
    tm.trust AS trust_name,
    tm.issuer,
    
    -- Time range
    tm.first_reporting_date,
    tm.latest_reporting_date,
    
    -- Original metrics
    tm.loan_count AS orig_loan_count,
    tm.total_orig_bal,
    
    -- Current metrics
    lm.current_loan_count,
    lm.delinquent_loans,
    lm.current_bal,
    CASE 
        WHEN tm.total_orig_bal > 0 
        THEN lm.current_bal / tm.total_orig_bal 
        ELSE NULL 
    END AS factor,
    
    -- Delinquency ratio
    CASE 
        WHEN lm.current_loan_count > 0 
        THEN lm.delinquent_loans / lm.current_loan_count::NUMERIC 
        ELSE 0 
    END AS delinquency_ratio
FROM trust_metadata tm
LEFT JOIN latest_metrics lm ON tm.trust = lm.trust AND lm.recency_rank = 1 