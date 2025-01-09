{{
    config(
        materialized='table',
        partition_by={
            'field': 'period_start',
            'data_type': 'date',
            'granularity': 'month'
        }
    )
}}

with risk_metrics as (
    select
        f.property_key,
        f.trust,
        f.loan_number,
        f.period_start,
        p.property_type,
        p.property_state,
        -- Current metrics
        f.current_loan_amount,
        f.loan_to_value_ratio,
        f.debt_service_coverage_ratio,
        f.physical_occupancy,
        -- Risk indicators
        case
            when f.loan_to_value_ratio > 0.8 then 'High'
            when f.loan_to_value_ratio > 0.6 then 'Medium'
            else 'Low'
        end as ltv_risk,
        case
            when f.debt_service_coverage_ratio < 1.2 then 'High'
            when f.debt_service_coverage_ratio < 1.5 then 'Medium'
            else 'Low'
        end as dscr_risk,
        case
            when f.physical_occupancy < 0.8 then 'High'
            when f.physical_occupancy < 0.9 then 'Medium'
            else 'Low'
        end as occupancy_risk,
        -- Trend analysis
        case
            when f.loan_amount_change > 0 then 'Increasing'
            when f.loan_amount_change < 0 then 'Decreasing'
            else 'Stable'
        end as loan_amount_trend,
        case
            when f.occupancy_change < -0.05 then 'Declining'
            when f.occupancy_change > 0.05 then 'Improving'
            else 'Stable'
        end as occupancy_trend,
        -- Composite risk score (1-10, higher is riskier)
        (
            case
                when f.loan_to_value_ratio > 0.8 then 4
                when f.loan_to_value_ratio > 0.6 then 2
                else 0
            end +
            case
                when f.debt_service_coverage_ratio < 1.2 then 4
                when f.debt_service_coverage_ratio < 1.5 then 2
                else 0
            end +
            case
                when f.physical_occupancy < 0.8 then 2
                when f.physical_occupancy < 0.9 then 1
                else 0
            end
        ) as risk_score
    from {{ ref('fct_loan_performance') }} f
    join {{ ref('dim_properties') }} p
        on f.property_key = p.property_key
),

aggregated_metrics as (
    select
        trust,
        property_type,
        property_state,
        period_start,
        -- Portfolio metrics
        count(*) as loan_count,
        sum(current_loan_amount) as total_loan_amount,
        avg(loan_to_value_ratio) as avg_ltv,
        avg(debt_service_coverage_ratio) as avg_dscr,
        avg(physical_occupancy) as avg_occupancy,
        -- Risk distribution
        sum(case when ltv_risk = 'High' then 1 else 0 end) / count(*)::float as high_ltv_pct,
        sum(case when dscr_risk = 'High' then 1 else 0 end) / count(*)::float as high_dscr_pct,
        sum(case when occupancy_risk = 'High' then 1 else 0 end) / count(*)::float as high_occupancy_risk_pct,
        -- Average risk score
        avg(risk_score) as avg_risk_score
    from risk_metrics
    group by 1, 2, 3, 4
)

select * from aggregated_metrics 