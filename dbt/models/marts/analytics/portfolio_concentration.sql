{{
    config(
        materialized='incremental',
        partition_by={
            'field': 'reporting_date',
            'data_type': 'date',
            'granularity': 'month'
        },
        post_hook = [
            "CREATE INDEX IF NOT EXISTS idx_{{ this.name }}_date ON {{ this }} (reporting_date)",
            "CREATE INDEX IF NOT EXISTS idx_{{ this.name }}_concentration_type ON {{ this }} (concentration_type, concentration_category)",
            "ANALYZE {{ this }}"
        ],
        unique_key=['reporting_date', 'concentration_type', 'concentration_category']
    )
}}

/*
 * Portfolio Concentration Analysis
 * 
 * This model analyzes loan concentrations across different dimensions by joining loan data
 * with property data and the geographic dimension hierarchy in the snowflake schema.
 * The model produces concentration metrics for property types, locations, and loan characteristics.
 */
WITH loan_data AS (
    SELECT
        -- Loan identifiers and metrics
        lp.loan_id,
        lp.reporting_date,
        lp.trust_id,
        lp.current_bal,
        
        -- Property attributes from property dimension
        dp.property_type_code,
        
        -- Geography attributes properly sourced via snowflake schema
        a.property_address,         -- from dim_geo_address
        z.zip_code,                 -- from dim_geo_zip
        c.city_name,                -- from dim_geo_city 
        s.state_code,               -- from dim_geo_state
        s.state_name,               -- from dim_geo_state
        s.region_name,              -- from dim_geo_state
        s.division_name,            -- from dim_geo_state
        
        -- Create loan size buckets
        CASE
            WHEN lp.current_bal < 1000000 THEN 'Under $1M'
            WHEN lp.current_bal < 5000000 THEN '$1M - $5M'
            WHEN lp.current_bal < 10000000 THEN '$5M - $10M'
            WHEN lp.current_bal < 25000000 THEN '$10M - $25M'
            WHEN lp.current_bal < 50000000 THEN '$25M - $50M'
            ELSE 'Over $50M'
        END AS loan_size_bucket,
        
        -- Create interest rate buckets
        CASE
            WHEN lp.current_intr_rate < 0.03 THEN 'Under 3%'
            WHEN lp.current_intr_rate < 0.04 THEN '3% - 4%'
            WHEN lp.current_intr_rate < 0.05 THEN '4% - 5%'
            WHEN lp.current_intr_rate < 0.06 THEN '5% - 6%'
            ELSE 'Over 6%'
        END AS interest_rate_bucket,
        
        -- Create remaining term buckets
        CASE
            WHEN lp.maturity_date <= lp.reporting_date THEN 'Matured'
            WHEN lp.maturity_date <= lp.reporting_date + INTERVAL '1 year' THEN 'Under 1 Year'
            WHEN lp.maturity_date <= lp.reporting_date + INTERVAL '3 years' THEN '1-3 Years'
            WHEN lp.maturity_date <= lp.reporting_date + INTERVAL '5 years' THEN '3-5 Years'
            WHEN lp.maturity_date <= lp.reporting_date + INTERVAL '10 years' THEN '5-10 Years'
            ELSE 'Over 10 Years'
        END AS remaining_term_bucket
    FROM {{ ref('fct_loan_monthly_performance') }} lp
    -- Join to property dimension
    LEFT JOIN {{ ref('dim_property') }} dp 
        ON lp.loan_id = dp.property_id
        AND lp.reporting_date >= dp.effective_date 
        AND lp.reporting_date < dp.end_date
    -- Join to geographic dimensions via snowflake schema
    LEFT JOIN {{ ref('dim_geo_address') }} a ON dp.geo_id = a.address_id
    LEFT JOIN {{ ref('dim_geo_zip') }} z ON a.zip_id = z.zip_id
    LEFT JOIN {{ ref('dim_geo_city') }} c ON z.city_id = c.city_id
    LEFT JOIN {{ ref('dim_geo_state') }} s ON c.state_id = s.state_id
    WHERE lp.reporting_date IS NOT NULL AND NOT lp.is_past_maturity
    {% if is_incremental() %}
    AND lp.reporting_date >= (SELECT max(reporting_date) FROM {{ this }})
    {% endif %}
),

-- Calculate totals per reporting date for percentages
reporting_date_totals AS (
    SELECT
        reporting_date,
        SUM(current_bal) AS total_balance,
        COUNT(DISTINCT loan_id) AS total_loans
    FROM loan_data
    GROUP BY reporting_date
),

-- Combine all concentration dimensions into a single query to reduce processing
combined_concentrations AS (
    -- Property Type Concentration
    SELECT
        reporting_date,
        'Property Type' AS concentration_type,
        COALESCE(property_type_code, 'Unknown') AS concentration_category,
        COUNT(DISTINCT loan_id) AS loan_count,
        SUM(current_bal) AS total_balance
    FROM loan_data
    GROUP BY reporting_date, property_type_code
    
    UNION ALL
    
    -- Geographic Concentration by State
    SELECT
        reporting_date,
        'State' AS concentration_type,
        COALESCE(state_name, 'Unknown') AS concentration_category,
        COUNT(DISTINCT loan_id) AS loan_count,
        SUM(current_bal) AS total_balance
    FROM loan_data
    GROUP BY reporting_date, state_name
    
    UNION ALL
    
    -- Geographic Concentration by City
    SELECT
        reporting_date,
        'City' AS concentration_type,
        COALESCE(city_name, 'Unknown') || ', ' || COALESCE(state_code, '') AS concentration_category,
        COUNT(DISTINCT loan_id) AS loan_count,
        SUM(current_bal) AS total_balance
    FROM loan_data
    WHERE city_name IS NOT NULL
    GROUP BY reporting_date, city_name, state_code
    
    UNION ALL
    
    -- Geographic Concentration by ZIP Code
    SELECT
        reporting_date,
        'ZIP Code' AS concentration_type,
        COALESCE(zip_code, 'Unknown') AS concentration_category,
        COUNT(DISTINCT loan_id) AS loan_count,
        SUM(current_bal) AS total_balance
    FROM loan_data
    WHERE zip_code IS NOT NULL
    GROUP BY reporting_date, zip_code
    
    UNION ALL
    
    -- Geographic Concentration by Address
    SELECT
        reporting_date,
        'Address' AS concentration_type,
        COALESCE(property_address, 'Unknown') || ', ' || COALESCE(city_name, '') || ', ' || COALESCE(state_code, '') AS concentration_category,
        COUNT(DISTINCT loan_id) AS loan_count,
        SUM(current_bal) AS total_balance
    FROM loan_data
    WHERE property_address IS NOT NULL
    GROUP BY reporting_date, property_address, city_name, state_code
    
    UNION ALL
    
    -- Geographic Concentration by Region
    SELECT
        reporting_date,
        'Region' AS concentration_type,
        COALESCE(region_name, 'Unknown') AS concentration_category,
        COUNT(DISTINCT loan_id) AS loan_count,
        SUM(current_bal) AS total_balance
    FROM loan_data
    GROUP BY reporting_date, region_name
    
    UNION ALL
    
    -- Geographic Concentration by Division
    SELECT
        reporting_date,
        'Division' AS concentration_type,
        COALESCE(division_name, 'Unknown') AS concentration_category,
        COUNT(DISTINCT loan_id) AS loan_count,
        SUM(current_bal) AS total_balance
    FROM loan_data
    GROUP BY reporting_date, division_name
    
    UNION ALL
    
    -- Loan Size Concentration
    SELECT
        reporting_date,
        'Loan Size' AS concentration_type,
        loan_size_bucket AS concentration_category,
        COUNT(DISTINCT loan_id) AS loan_count,
        SUM(current_bal) AS total_balance
    FROM loan_data
    GROUP BY reporting_date, loan_size_bucket
    
    UNION ALL
    
    -- Interest Rate Concentration
    SELECT
        reporting_date,
        'Interest Rate' AS concentration_type,
        interest_rate_bucket AS concentration_category,
        COUNT(DISTINCT loan_id) AS loan_count,
        SUM(current_bal) AS total_balance
    FROM loan_data
    GROUP BY reporting_date, interest_rate_bucket
    
    UNION ALL
    
    -- Remaining Term Concentration
    SELECT
        reporting_date,
        'Remaining Term' AS concentration_type,
        remaining_term_bucket AS concentration_category,
        COUNT(DISTINCT loan_id) AS loan_count,
        SUM(current_bal) AS total_balance
    FROM loan_data
    GROUP BY reporting_date, remaining_term_bucket
)

-- Final output with percentage calculations
SELECT
    -- Identifiers
    c.reporting_date,
    c.concentration_type,
    c.concentration_category,
    
    -- Metrics
    c.loan_count,
    c.total_balance,
    c.total_balance / NULLIF(c.loan_count, 0)::FLOAT AS avg_loan_balance,
    
    -- Percentage metrics  
    c.loan_count / NULLIF(t.total_loans, 0)::FLOAT AS pct_of_loans,
    c.total_balance / NULLIF(t.total_balance, 0)::FLOAT AS pct_of_balance,
    
    -- Ranking metrics
    RANK() OVER (
        PARTITION BY c.reporting_date, c.concentration_type 
        ORDER BY c.total_balance DESC
    ) AS rank_by_balance,
    
    -- Flags
    CASE WHEN RANK() OVER (
        PARTITION BY c.reporting_date, c.concentration_type 
        ORDER BY c.total_balance DESC
    ) <= 5 THEN TRUE ELSE FALSE END AS is_top_5,
    
    -- Metadata
    CURRENT_TIMESTAMP AS loaded_at
FROM combined_concentrations c
JOIN reporting_date_totals t ON c.reporting_date = t.reporting_date
ORDER BY c.reporting_date, c.concentration_type, c.total_balance DESC 