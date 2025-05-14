{#
# Fill SCD Value Macro
#
# This macro implements a fill-forward strategy specifically designed for Slowly Changing Dimension (SCD) patterns
# where values are reported sporadically and need to be carried forward until the next reported value.
#
# Arguments:
#   - column_name (string): The name of the column to fill
#   - partition_by (list): Columns to partition by, defaults to ['asset_num', 'property_num']
#   - order_by (string): Column to order by within partitions, defaults to 'reporting_period_end_date'
#
# Example Usage:
#   SELECT 
#       asset_num,
#       property_num,
#       reporting_period_end_date,
#       {{ fill_scd_value('most_recent_revenue_amt') }} as filled_revenue,
#       {{ fill_scd_value(
#           'most_recent_noi_amt', 
#           partition_by=['asset_num', 'property_num', 'trust_id']
#       ) }} as filled_noi
#   FROM properties
#
# The macro will:
# 1. Take the most recent non-null value for each partition
# 2. If no previous values exist in the partition, use the first non-null value
# 3. Handle cases with multiple sequential nulls correctly
#}

{% macro fill_scd_value(column_name, partition_by=['asset_num', 'property_num'], order_by='reporting_period_end_date') %}
    COALESCE(
        (ARRAY_REMOVE(
            (ARRAY_AGG({{ column_name }}) OVER(
                PARTITION BY {% for col in partition_by %}{{ col }}{% if not loop.last %}, {% endif %}{% endfor %}
                ORDER BY {{ order_by }}
            )), 
            NULL
        ))[array_upper(ARRAY_REMOVE(
            (ARRAY_AGG({{ column_name }}) OVER(
                PARTITION BY {% for col in partition_by %}{{ col }}{% if not loop.last %}, {% endif %}{% endfor %}
                ORDER BY {{ order_by }}
            )), 
            NULL
        ),1)],
        FIRST_VALUE({{ column_name }}) OVER (
            PARTITION BY {% for col in partition_by %}{{ col }}{% if not loop.last %}, {% endif %}{% endfor %}
            ORDER BY CASE WHEN {{ column_name }} IS NOT NULL THEN {{ order_by }} END
        )
    )
{% endmacro %} 