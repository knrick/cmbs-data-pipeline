{{
    config(
        materialized='table',
        unique_key='property_key'
    )
}}

with properties as (
    select distinct
        trust,
        loan_number,
        property_name,
        property_address,
        property_city,
        property_state,
        property_zip,
        property_type,
        net_rentable_square_feet,
        number_of_units,
        year_built
    from {{ ref('stg_cmbs_loans') }}
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key([
            'trust',
            'loan_number',
            'property_address'
        ]) }} as property_key,
        *,
        current_timestamp() as dbt_updated_at
    from properties
)

select * from final 