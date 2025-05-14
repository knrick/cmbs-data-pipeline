{{
  config(
    materialized = 'incremental',
    unique_key = ['loan_id', 'modification_date'],
    post_hook = [
      "CREATE INDEX IF NOT EXISTS idx_{{ this.name }}_loan_date ON {{ this }} (loan_id, modification_date)",
      "CREATE INDEX IF NOT EXISTS idx_{{ this.name }}_trust_date ON {{ this }} (trust_id, modification_date)",
      "ANALYZE {{ this }}"
    ]
  )
}}

WITH loan_monthly_data AS (
    SELECT DISTINCT ON (asset_num, reporting_period_end_date)
        asset_num AS loan_id,
        reporting_period_end_date AS reporting_date,
        trust AS trust_id,
        company AS issuer,
        report_period_intr_rate_pct AS current_intr_rate,
        report_period_end_scheduled_loan_bal_amt AS current_bal,
        report_period_modification_indicator,
        modified_indicator,
        {{ fill_scd_value('maturity_date', ['asset_num']) }} AS maturity_date
    FROM {{ source('cmbs', 'loans') }}
    WHERE reporting_period_end_date IS NOT NULL
    
    {% if is_incremental() %}
      -- Only process new data since last run
      AND reporting_period_end_date > (
        SELECT COALESCE(MAX(modification_date), '2000-01-01'::date) FROM {{ this }}
      )
    {% endif %}
),

-- Track metrics over time to detect changes
loan_changes AS (
    SELECT
        loan_id,
        reporting_date,
        trust_id,
        issuer,
        current_intr_rate,
        current_bal,
        maturity_date,
        COALESCE(report_period_modification_indicator, FALSE) AS report_period_modification_indicator,
        COALESCE(modified_indicator, FALSE) AS modified_indicator,
        
        -- Previous period metrics using window functions
        LAG(current_intr_rate) OVER (
            PARTITION BY loan_id 
            ORDER BY reporting_date
        ) AS previous_intr_rate,
        
        LAG(maturity_date) OVER (
            PARTITION BY loan_id 
            ORDER BY reporting_date
        ) AS previous_maturity_date,
        
        LAG(reporting_date) OVER (
            PARTITION BY loan_id 
            ORDER BY reporting_date
        ) AS previous_reporting_date
    FROM loan_monthly_data
),

-- Detect modifications
modifications AS (
    SELECT
        loan_id,
        reporting_date AS modification_date,
        trust_id,
        issuer,
        current_intr_rate AS new_intr_rate,
        previous_intr_rate,
        COALESCE(current_intr_rate, 0) - COALESCE(previous_intr_rate, 0) AS intr_rate_change,
        maturity_date AS new_maturity_date,
        previous_maturity_date,
        current_bal,
        CASE
            WHEN report_period_modification_indicator = TRUE THEN 'This Period'
            WHEN modified_indicator = TRUE AND previous_reporting_date IS NOT NULL THEN 'Existing'
            ELSE 'Unknown'
        END AS modification_type,
        CASE
            WHEN current_intr_rate IS NOT NULL AND previous_intr_rate IS NOT NULL AND
                 ABS(current_intr_rate - previous_intr_rate) > 0.0001 THEN TRUE 
            ELSE FALSE
        END AS has_rate_change,
        CASE
            WHEN maturity_date IS NOT NULL AND previous_maturity_date IS NOT NULL AND
                 maturity_date != previous_maturity_date THEN TRUE
            ELSE FALSE
        END AS has_maturity_change
    FROM loan_changes
    WHERE (
        -- Detection logic for modifications
        (report_period_modification_indicator = TRUE OR modified_indicator = TRUE)
        AND previous_reporting_date IS NOT NULL
        AND (
            -- Rate changed
            (previous_intr_rate IS NOT NULL AND current_intr_rate IS NOT NULL AND 
             ABS(current_intr_rate - previous_intr_rate) > 0.0001)
            OR
            -- Maturity date changed
            (previous_maturity_date IS NOT NULL AND maturity_date IS NOT NULL AND 
             maturity_date != previous_maturity_date)
        )
    )
)

SELECT
    -- Surrogate key using dbt_utils
    {{ dbt_utils.generate_surrogate_key(['loan_id', 'modification_date']) }} AS modification_id,
    
    -- Foreign keys for dimension tables
    loan_id,
    modification_date,
    trust_id,
    
    -- Modification details
    modification_type,
    has_rate_change,
    has_maturity_change,
    
    -- Interest rate changes
    previous_intr_rate,
    new_intr_rate,
    intr_rate_change,
    
    -- Maturity changes
    previous_maturity_date,
    new_maturity_date,
    CASE 
        WHEN previous_maturity_date IS NOT NULL AND new_maturity_date IS NOT NULL
        THEN new_maturity_date - previous_maturity_date
        ELSE NULL
    END AS maturity_extension_days,
    
    -- Balance at time of modification
    current_bal,
    
    -- Timestamp
    CURRENT_TIMESTAMP AS loaded_at
FROM modifications
WHERE
    has_rate_change = TRUE OR has_maturity_change = TRUE 