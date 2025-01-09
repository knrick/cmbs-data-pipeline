{{
    config(
        materialized='incremental',
        unique_key=['property_key', 'period_start'],
        partition_by={
            'field': 'period_start',
            'data_type': 'date',
            'granularity': 'month'
        }
    )
}}

with loan_metrics as (
    select
        p.property_key,
        l.trust,
        l.loan_number,
        l.period_start,
        l.period_end,
        -- Loan performance metrics
        l.current_loan_amount,
        l.interest_rate,
        l.debt_service_coverage_ratio,
        l.most_recent_debt_service_coverage_ratio,
        l.most_recent_debt_service_coverage_ratio_date,
        l.physical_occupancy,
        l.loan_status,
        -- Valuation metrics
        l.valuation_amount,
        l.valuation_date,
        -- Calculate LTV
        l.current_loan_amount / nullif(l.valuation_amount, 0) as loan_to_value_ratio,
        -- Calculate month-over-month changes
        l.current_loan_amount - lag(l.current_loan_amount) over (
            partition by l.trust, l.loan_number 
            order by l.period_start
        ) as loan_amount_change,
        l.physical_occupancy - lag(l.physical_occupancy) over (
            partition by l.trust, l.loan_number 
            order by l.period_start
        ) as occupancy_change
    from {{ ref('stg_cmbs_loans') }} l
    join {{ ref('dim_properties') }} p
        on l.trust = p.trust
        and l.loan_number = p.loan_number
    {% if is_incremental() %}
    where l.period_start > (select max(period_start) from {{ this }})
    {% endif %}
)

select * from loan_metrics 