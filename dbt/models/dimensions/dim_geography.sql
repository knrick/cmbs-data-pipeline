{{
  config(
    materialized = 'table',
    post_hook = [
      "CREATE INDEX IF NOT EXISTS idx_{{ this.name }}_geo_id ON {{ this }} (geo_id)",
      "CREATE INDEX IF NOT EXISTS idx_{{ this.name }}_state ON {{ this }} (state)",
      "ANALYZE {{ this }}"
    ]
  )
}}

WITH distinct_locations AS (
    SELECT DISTINCT
        property_state,
        property_city,
        property_zip,
        property_county
    FROM {{ source('cmbs', 'properties') }}
    WHERE property_state IS NOT NULL 
),

-- Add ROW_NUMBER to handle potential duplicates even after selecting DISTINCT
numbered_locations AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY 
                {{ dbt_utils.generate_surrogate_key(['property_state', 'property_city', 'property_zip']) }}
            ORDER BY 
                property_county
        ) as location_rank
    FROM distinct_locations
)

SELECT
    -- Create a surrogate key for the geography using dbt_utils
    {{ dbt_utils.generate_surrogate_key(['property_state', 'property_city', 'property_zip']) }} AS geo_id,
    
    -- Location details
    property_state AS state,
    property_city AS city,
    property_zip AS zip_code,
    property_county AS county,
    
    -- Region classification
    CASE 
        WHEN property_state IN ('ME', 'NH', 'VT', 'MA', 'RI', 'CT') THEN 'New England'
        WHEN property_state IN ('NY', 'NJ', 'PA') THEN 'Mid-Atlantic'
        WHEN property_state IN ('OH', 'MI', 'IN', 'IL', 'WI', 'MN', 'IA', 'MO', 'ND', 'SD', 'NE', 'KS') THEN 'Midwest'
        WHEN property_state IN ('DE', 'MD', 'DC', 'VA', 'WV', 'NC', 'SC', 'GA', 'FL', 'KY', 'TN', 'AL', 'MS', 'AR', 'LA', 'OK', 'TX') THEN 'South'
        WHEN property_state IN ('MT', 'ID', 'WY', 'CO', 'NM', 'AZ', 'UT', 'NV') THEN 'Mountain West'
        WHEN property_state IN ('WA', 'OR', 'CA', 'AK', 'HI') THEN 'Pacific'
        ELSE 'Other'
    END AS region,
    
    -- Division classification (based on Census Bureau divisions)
    CASE 
        WHEN property_state IN ('ME', 'NH', 'VT', 'MA', 'RI', 'CT') THEN 'New England'
        WHEN property_state IN ('NY', 'NJ', 'PA') THEN 'Middle Atlantic'
        WHEN property_state IN ('OH', 'MI', 'IN', 'IL', 'WI') THEN 'East North Central'
        WHEN property_state IN ('MN', 'IA', 'MO', 'ND', 'SD', 'NE', 'KS') THEN 'West North Central'
        WHEN property_state IN ('DE', 'MD', 'DC', 'VA', 'WV', 'NC', 'SC', 'GA', 'FL') THEN 'South Atlantic'
        WHEN property_state IN ('KY', 'TN', 'AL', 'MS') THEN 'East South Central'
        WHEN property_state IN ('AR', 'LA', 'OK', 'TX') THEN 'West South Central'
        WHEN property_state IN ('MT', 'ID', 'WY', 'CO', 'NM', 'AZ', 'UT', 'NV') THEN 'Mountain'
        WHEN property_state IN ('WA', 'OR', 'CA', 'AK', 'HI') THEN 'Pacific'
        ELSE 'Other'
    END AS division,
    
    -- Metro classification (simplified)
    CASE
        WHEN property_city IN ('New York', 'Los Angeles', 'Chicago', 'Dallas', 'Houston', 
                            'Washington', 'Philadelphia', 'Miami', 'Atlanta', 'Boston',
                            'San Francisco', 'Phoenix', 'Seattle', 'Minneapolis', 'San Diego',
                            'Denver', 'Orlando', 'Tampa', 'Portland', 'Las Vegas') THEN 'Major Metro'
        ELSE 'Other Market'
    END AS market_size
FROM numbered_locations
WHERE location_rank = 1 -- Only keep one record per geo_id 