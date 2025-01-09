{{
    config(
        materialized='incremental',
        unique_key=['trust', 'loan_number', 'period_start'],
        partition_by={
            'field': 'period_start',
            'data_type': 'date',
            'granularity': 'month'
        }
    )
}}

with source as (
    select
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
        year_built,
        original_loan_amount,
        current_loan_amount,
        interest_rate,
        payment_frequency,
        payment_type,
        period_start,
        period_end,
        record_count,
        -- Additional fields from compressed data
        valuation_amount,
        valuation_date,
        physical_occupancy,
        loan_status,
        debt_service_coverage_ratio,
        most_recent_debt_service_coverage_ratio,
        most_recent_debt_service_coverage_ratio_date
    from {{ source('cmbs', 'compressed_loans') }}
    {% if is_incremental() %}
    where period_start > (select max(period_start) from {{ this }})
    {% endif %}
) 