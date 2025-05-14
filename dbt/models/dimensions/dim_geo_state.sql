{{
  config(
    materialized = 'table',
    unique_key = 'state_id',
    post_hook = [
      "CREATE INDEX IF NOT EXISTS idx_{{ this.name }}_state_code ON {{ this }} (state_code)",
      "ANALYZE {{ this }}"
    ]
  )
}}

-- Clean, self-contained state dimension with no dependencies on dim_geography
SELECT 
  {{ dbt_utils.generate_surrogate_key(['state_code']) }} AS state_id,
  state_code,
  state_name,
  region_name,
  division_name
FROM (
  VALUES
    -- Northeast - New England
    ('CT', 'Connecticut', 'Northeast', 'New England'),
    ('ME', 'Maine', 'Northeast', 'New England'),
    ('MA', 'Massachusetts', 'Northeast', 'New England'),
    ('NH', 'New Hampshire', 'Northeast', 'New England'),
    ('RI', 'Rhode Island', 'Northeast', 'New England'),
    ('VT', 'Vermont', 'Northeast', 'New England'),
    
    -- Northeast - Mid-Atlantic
    ('NJ', 'New Jersey', 'Northeast', 'Mid-Atlantic'),
    ('NY', 'New York', 'Northeast', 'Mid-Atlantic'),
    ('PA', 'Pennsylvania', 'Northeast', 'Mid-Atlantic'),
    
    -- Midwest - East North Central
    ('IL', 'Illinois', 'Midwest', 'East North Central'),
    ('IN', 'Indiana', 'Midwest', 'East North Central'),
    ('MI', 'Michigan', 'Midwest', 'East North Central'),
    ('OH', 'Ohio', 'Midwest', 'East North Central'),
    ('WI', 'Wisconsin', 'Midwest', 'East North Central'),
    
    -- Midwest - West North Central
    ('IA', 'Iowa', 'Midwest', 'West North Central'),
    ('KS', 'Kansas', 'Midwest', 'West North Central'),
    ('MN', 'Minnesota', 'Midwest', 'West North Central'),
    ('MO', 'Missouri', 'Midwest', 'West North Central'),
    ('NE', 'Nebraska', 'Midwest', 'West North Central'),
    ('ND', 'North Dakota', 'Midwest', 'West North Central'),
    ('SD', 'South Dakota', 'Midwest', 'West North Central'),
    
    -- South - South Atlantic
    ('DE', 'Delaware', 'South', 'South Atlantic'),
    ('FL', 'Florida', 'South', 'South Atlantic'),
    ('GA', 'Georgia', 'South', 'South Atlantic'),
    ('MD', 'Maryland', 'South', 'South Atlantic'),
    ('NC', 'North Carolina', 'South', 'South Atlantic'),
    ('SC', 'South Carolina', 'South', 'South Atlantic'),
    ('VA', 'Virginia', 'South', 'South Atlantic'),
    ('DC', 'District of Columbia', 'South', 'South Atlantic'),
    ('WV', 'West Virginia', 'South', 'South Atlantic'),
    
    -- South - East South Central
    ('AL', 'Alabama', 'South', 'East South Central'),
    ('KY', 'Kentucky', 'South', 'East South Central'),
    ('MS', 'Mississippi', 'South', 'East South Central'),
    ('TN', 'Tennessee', 'South', 'East South Central'),
    
    -- South - West South Central
    ('AR', 'Arkansas', 'South', 'West South Central'),
    ('LA', 'Louisiana', 'South', 'West South Central'),
    ('OK', 'Oklahoma', 'South', 'West South Central'),
    ('TX', 'Texas', 'South', 'West South Central'),
    
    -- West - Mountain
    ('AZ', 'Arizona', 'West', 'Mountain'),
    ('CO', 'Colorado', 'West', 'Mountain'),
    ('ID', 'Idaho', 'West', 'Mountain'),
    ('MT', 'Montana', 'West', 'Mountain'),
    ('NV', 'Nevada', 'West', 'Mountain'),
    ('NM', 'New Mexico', 'West', 'Mountain'),
    ('UT', 'Utah', 'West', 'Mountain'),
    ('WY', 'Wyoming', 'West', 'Mountain'),
    
    -- West - Pacific
    ('CA', 'California', 'West', 'Pacific'),
    ('OR', 'Oregon', 'West', 'Pacific'),
    ('WA', 'Washington', 'West', 'Pacific'),
    
    -- Other - Non-Contiguous
    ('AK', 'Alaska', 'Other', 'Non-Contiguous'),
    ('HI', 'Hawaii', 'Other', 'Non-Contiguous'),
    ('PR', 'Puerto Rico', 'Other', 'Non-Contiguous'),
    ('VI', 'Virgin Islands', 'Other', 'Non-Contiguous'),
    ('GU', 'Guam', 'Other', 'Non-Contiguous'),
    ('MP', 'Northern Mariana Islands', 'Other', 'Non-Contiguous'),
    ('AS', 'American Samoa', 'Other', 'Non-Contiguous'),

    -- Unknown/Not Applicable
    ('NA', 'Not Applicable', 'Unknown', 'Unknown'),
    ('UNK', 'Unknown', 'Unknown', 'Unknown')
) AS states(state_code, state_name, region_name, division_name)