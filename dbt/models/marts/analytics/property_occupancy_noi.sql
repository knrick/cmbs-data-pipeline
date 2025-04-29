{{
  config(
    materialized = 'table',
    schema = 'mart',
    indexes = [
      {'columns': ['property_id']},
      {'columns': ['reporting_date']},
      {'columns': ['property_type_code']}
    ]
  )
}}

WITH property_metrics AS (
    SELECT
        pm.property_id,
        pm.reporting_date,
        p.property_type_code,
        p.year_built,
        p.zip_id,
        s.state_code as state,
        s.region_name as region,
        pm.current_occupancy_pct as current_occupancy,
        pm.securitization_occupancy_pct as securitization_occupancy,
        pm.current_revenue,
        pm.current_expenses,
        pm.current_noi,
        pm.square_feet
    FROM {{ ref('fct_property_metrics') }} pm
    LEFT JOIN {{ ref('dim_property') }} p ON pm.property_id = p.property_id
    LEFT JOIN {{ ref('dim_geo_zip') }} z ON p.zip_id = z.zip_id
    LEFT JOIN {{ ref('dim_geo_city') }} c ON z.city_id = c.city_id
    LEFT JOIN {{ ref('dim_geo_state') }} s ON c.state_id = s.state_id
),

occupancy_trends AS (
    SELECT
        pm.property_id,
        pm.property_type_code,
        pm.reporting_date,
        pm.state,
        pm.region,
        pm.current_occupancy,
        pm.securitization_occupancy,
        -- Calculate occupancy change
        pm.current_occupancy - pm.securitization_occupancy as occupancy_change,
        -- Calculate rolling average occupancy
        AVG(pm.current_occupancy) OVER (
            PARTITION BY pm.property_id 
            ORDER BY pm.reporting_date 
            ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
        ) as trailing_12m_avg_occupancy,
        -- Calculate occupancy volatility
        STDDEV(pm.current_occupancy) OVER (
            PARTITION BY pm.property_id 
            ORDER BY pm.reporting_date 
            ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
        ) as occupancy_volatility_12m,
        -- Pass through square footage for calculations
        pm.square_feet
    FROM property_metrics pm
),

noi_analysis AS (
    SELECT
        pm.property_id,
        pm.property_type_code,
        pm.reporting_date,
        pm.state,
        pm.region,
        pm.current_revenue,
        pm.current_expenses,
        pm.current_noi,
        -- Calculate NOI per square foot
        pm.current_noi / NULLIF(pm.square_feet, 0) as noi_per_sqft,
        -- Calculate expense ratio
        pm.current_expenses / NULLIF(pm.current_revenue, 0) as expense_ratio,
        -- Calculate rolling 12-month metrics
        SUM(pm.current_noi) OVER (
            PARTITION BY pm.property_id 
            ORDER BY pm.reporting_date 
            ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
        ) as trailing_12m_noi,
        -- Calculate YoY NOI growth
        (pm.current_noi - LAG(pm.current_noi, 12) OVER (
            PARTITION BY pm.property_id 
            ORDER BY pm.reporting_date
        )) / NULLIF(LAG(pm.current_noi, 12) OVER (
            PARTITION BY pm.property_id 
            ORDER BY pm.reporting_date
        ), 0) * 100 as noi_yoy_growth,
        -- Pass through square footage for calculations
        pm.square_feet
    FROM property_metrics pm
)

SELECT
    ot.property_id,
    ot.reporting_date,
    ot.property_type_code,
    ot.state,
    ot.region,
    ot.current_occupancy,
    ot.securitization_occupancy,
    ot.occupancy_change,
    ot.trailing_12m_avg_occupancy,
    ot.occupancy_volatility_12m,
    na.current_revenue,
    na.current_expenses,
    na.current_noi,
    na.noi_per_sqft,
    na.expense_ratio,
    na.trailing_12m_noi,
    na.noi_yoy_growth,
    ot.square_feet,
    -- Property type averages
    AVG(ot.current_occupancy) OVER (
        PARTITION BY ot.property_type_code, DATE_TRUNC('month', ot.reporting_date)
    ) as avg_occupancy_by_property_type,
    AVG(na.noi_per_sqft) OVER (
        PARTITION BY ot.property_type_code, DATE_TRUNC('month', ot.reporting_date)
    ) as avg_noi_per_sqft_by_property_type,
    -- Market averages (by state)
    AVG(ot.current_occupancy) OVER (
        PARTITION BY ot.state, DATE_TRUNC('month', ot.reporting_date)
    ) as avg_occupancy_by_state,
    AVG(na.noi_per_sqft) OVER (
        PARTITION BY ot.state, DATE_TRUNC('month', ot.reporting_date)
    ) as avg_noi_per_sqft_by_state
FROM occupancy_trends ot
JOIN noi_analysis na ON 
    ot.property_id = na.property_id AND 
    ot.reporting_date = na.reporting_date