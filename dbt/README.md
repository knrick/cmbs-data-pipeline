# CMBS Analytics - DBT Transformation Project

This DBT project contains transformations for CMBS (Commercial Mortgage-Backed Securities) data obtained from SEC EDGAR filings.

## Project Structure

The project follows a dimensional modeling approach with a star schema:

```
models/
├── dimensions/          # Dimension tables
│   ├── dim_date.sql
│   ├── dim_trust.sql
│   ├── dim_property.sql
│   └── dim_geography.sql
├── facts/               # Fact tables
│   ├── fct_loan_monthly_performance.sql
│   ├── fct_property_metrics.sql
│   └── fct_loan_modifications.sql
├── marts/               # Business-specific aggregated models
│   └── analytics/       # Analytics mart models
├── metrics.yml          # Metric definitions for MetricFlow
└── sources.yml          # Source definitions
```

## Data Model Overview

### Dimension Tables

1. **dim_date**: Calendar dimension containing all reporting dates and useful date attributes.
2. **dim_trust**: Details about each CMBS trust with key metadata.
3. **dim_property**: Property information derived from the latest property records.
4. **dim_geography**: Normalized geographical information for location-based analysis.

### Fact Tables

1. **fct_loan_monthly_performance**: Monthly performance metrics for each loan.
2. **fct_property_metrics**: Performance metrics for each property over time.
3. **fct_loan_modifications**: Records changes to loan terms such as interest rates and maturity dates.

### Semantic Layer (MetricFlow)

This project uses MetricFlow to define a semantic layer on top of the data model. Metrics are defined in the `metrics.yml` file, which includes:

1. **Metrics**: Define business metrics used for analysis, including:
   - **total_current_balance**: Total outstanding loan balance
   - **total_delinquent_balance**: Total balance of delinquent loans
   - **delinquency_rate**: Percentage of balance that is delinquent
   - **average_occupancy**: Average property occupancy percentage
   - **average_dscr**: Average debt service coverage ratio
   - **total_loan_count**: Total number of loans
   - **average_interest_rate**: Average interest rate across loans
   - **total_property_count**: Total number of properties
   - **average_valuation_change**: Average percentage change in property valuation

## Getting Started

1. Set up your profile following the guidance in `profiles_guide.md`
2. Install dependencies:
   ```
   dbt deps
   pip install dbt-metricflow
   ```

3. Create DBT_PROFILES_DIR environment variable:
   ```
   export DBT_PROFILES_DIR=~/.dbt
   ```

4. Build the models:
   ```
   dbt build
   ```

4. Validate the semantic layer:
   ```
   mf validate-configs
   ```

5. List available metrics:
   ```
   mf list metrics
   ```

## Using the Semantic Layer

The semantic layer provides a standardized way to query metrics. Here are some examples:

### Query Total Current Balance Over Time
```bash
mf query --metrics total_current_balance --group-by metric_time
```

### Query Delinquency Rate by Loan Status
```bash
mf query --metrics delinquency_rate --group-by loan__loan_status
```

### Query Metrics with Filters
```bash
mf query --metrics total_current_balance --group-by metric_time --where "loan__days_past_due > 0"
```

## Common SQL Analyses

Here are some common analyses you can perform with this data model:

### Delinquency Analysis
```sql
SELECT 
    d.year,
    d.month,
    dt.trust_name,
    SUM(lp.current_balance) as total_balance,
    SUM(CASE WHEN lp.days_past_due > 0 THEN lp.current_balance ELSE 0 END) as delinquent_balance,
    SUM(CASE WHEN lp.days_past_due > 0 THEN lp.current_balance ELSE 0 END) / 
        NULLIF(SUM(lp.current_balance), 0) as delinquency_rate
FROM {{ ref('fct_loan_monthly_performance') }} lp
JOIN {{ ref('dim_date') }} d ON lp.reporting_date = d.date_id
JOIN {{ ref('dim_trust') }} dt ON lp.trust_id = dt.trust_id
GROUP BY d.year, d.month, dt.trust_name
ORDER BY d.year, d.month, dt.trust_name
```

### Property Performance by Type
```sql
SELECT
    dp.property_type_description,
    AVG(pm.current_occupancy_pct) as avg_occupancy,
    AVG(pm.current_dscr) as avg_dscr,
    AVG(pm.revenue_per_sqft) as avg_revenue_per_sqft
FROM {{ ref('fct_property_metrics') }} pm
JOIN {{ ref('dim_property') }} dp ON pm.property_id = dp.property_id
WHERE pm.reporting_date > '2020-01-01'
GROUP BY dp.property_type_description
ORDER BY avg_revenue_per_sqft DESC
```

### Modification Impact Analysis
```sql
-- Analyze performance after modifications
WITH modified_loans AS (
    SELECT
        loan_id,
        modification_date,
        interest_rate_change
    FROM {{ ref('fct_loan_modifications') }}
    WHERE has_rate_change = TRUE
)

SELECT
    CASE
        WHEN m.interest_rate_change < -1 THEN 'Large Rate Decrease (>1%)'
        WHEN m.interest_rate_change < 0 THEN 'Rate Decrease'
        WHEN m.interest_rate_change > 1 THEN 'Large Rate Increase (>1%)'
        WHEN m.interest_rate_change > 0 THEN 'Rate Increase'
        ELSE 'No Change'
    END as modification_type,
    
    -- Performance 3 months after modification
    AVG(CASE 
        WHEN lp.reporting_date BETWEEN m.modification_date AND (m.modification_date + INTERVAL '3 months')
        THEN lp.days_past_due
    END) as avg_dpd_3m_after,
    
    -- Performance 6 months after modification
    AVG(CASE 
        WHEN lp.reporting_date BETWEEN m.modification_date AND (m.modification_date + INTERVAL '6 months')
        THEN CASE WHEN lp.days_past_due = 0 THEN 1 ELSE 0 END
    END) as pct_current_6m_after,
    
    COUNT(DISTINCT m.loan_id) as loan_count
FROM modified_loans m
JOIN {{ ref('fct_loan_monthly_performance') }} lp ON 
    m.loan_id = lp.loan_id AND
    lp.reporting_date >= m.modification_date
GROUP BY modification_type
ORDER BY modification_type
```

## Future Enhancements

1. Expand the semantic layer with additional metrics
2. Create visualization models in Lightdash using MetricFlow metrics
3. Add cross-trust aggregations for market analysis
4. Implement additional data quality tests
5. Prepare for migration to Snowflake

## Maintenance

- Run daily/weekly incremental updates
- Validate data quality after each update
- Validate the semantic layer after schema changes
- Document new metrics and analyses

## Contributors

- Dmitry Alferov

## License

This project is private and confidential. 