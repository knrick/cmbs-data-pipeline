{{
  config(
    materialized = 'table',
    post_hook = [
      "CREATE INDEX IF NOT EXISTS idx_{{ this.name }}_date_day ON {{ this }} (date_day)",
      "ANALYZE {{ this }}"
    ]
  )
}}

WITH date_spine AS (
    {{
        dbt.date_spine(
            'day',
            "make_date(2000, 1, 1)",
            "make_date(2030, 12, 31)"
        )
    }}
)

SELECT
    -- Primary key  
    date_day AS date_id,
    
    -- Standard date attributes
    date_day,
    EXTRACT(YEAR FROM date_day) AS year,
    EXTRACT(QUARTER FROM date_day) AS quarter,
    EXTRACT(MONTH FROM date_day) AS month,
    TO_CHAR(date_day, 'Month') AS month_name,
    EXTRACT(DAY FROM date_day) AS day_of_month,
    EXTRACT(DOW FROM date_day) AS day_of_week,
    TO_CHAR(date_day, 'Day') AS day_of_week_name,
    (date_day = DATE_TRUNC('MONTH', date_day) + INTERVAL '1 MONTH - 1 day')::boolean AS is_end_of_month,
    
    -- Fiscal periods (assuming Oct-Sep fiscal year)
    CASE
        WHEN EXTRACT(MONTH FROM date_day) BETWEEN 7 AND 9 THEN 1
        WHEN EXTRACT(MONTH FROM date_day) BETWEEN 10 AND 12 THEN 2
        WHEN EXTRACT(MONTH FROM date_day) BETWEEN 1 AND 3 THEN 3
        WHEN EXTRACT(MONTH FROM date_day) BETWEEN 4 AND 6 THEN 4
    END AS fiscal_quarter,
    
    CASE
        WHEN EXTRACT(MONTH FROM date_day) >= 7 
        THEN EXTRACT(YEAR FROM date_day) + 1
        ELSE EXTRACT(YEAR FROM date_day)
    END AS fiscal_year,
    
    -- Additional calculated fields
    DATE_TRUNC('MONTH', date_day)::DATE AS first_day_of_month,
    (DATE_TRUNC('MONTH', date_day) + INTERVAL '1 MONTH - 1 day')::DATE AS last_day_of_month,
    DATE_TRUNC('QUARTER', date_day)::DATE AS first_day_of_quarter,
    (DATE_TRUNC('QUARTER', date_day) + INTERVAL '3 MONTHS - 1 day')::DATE AS last_day_of_quarter,
    DATE_TRUNC('YEAR', date_day)::DATE AS first_day_of_year,
    (DATE_TRUNC('YEAR', date_day) + INTERVAL '1 YEAR - 1 day')::DATE AS last_day_of_year,
    
    -- Holiday flags (simplified version without dbt_date)
    FALSE AS is_holiday,
    NULL AS holiday_name,
    
    -- Business day flag
    EXTRACT(DOW FROM date_day) NOT IN (0, 6) AS is_business_day
FROM date_spine 