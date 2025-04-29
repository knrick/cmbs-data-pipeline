# DBT Profiles Setup Guide

This guide will help you set up your DBT profiles for connecting to your Postgres database (and later Snowflake).

## Setting Up Your Profile

Create a `profiles.yml` file in your `~/.dbt/` directory with the following content:

```yaml
cmbs_analytics:
  target: dev
  outputs:
    dev:
      type: postgres
      host: "{{ env_var('POSTGRES_HOST', 'localhost') }}"
      port: "{{ env_var('POSTGRES_PORT', 5432) | as_number }}"
      user: "{{ env_var('POSTGRES_USER', 'postgres') }}"
      password: "{{ env_var('POSTGRES_PASSWORD', 'postgres') }}"
      dbname: "{{ env_var('POSTGRES_DB', 'cmbs_data') }}"
      schema: public
      threads: 4
      keepalives_idle: 0
      connect_timeout: 10
      retries: 1
```

This profile uses environment variables with fallbacks to default values, which makes it portable and secure.

## Environment Variables

You can set these environment variables in your shell or in a `.env` file:

```
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_USER=postgres
POSTGRES_PASSWORD=your_password_here
POSTGRES_DB=cmbs_data
```

## Testing Your Connection

To test that your connection works, run:

```bash
dbt debug
```

## MetricFlow Configuration

This project uses MetricFlow to create a semantic layer on top of the dbt models. MetricFlow requires:

1. Installing the package:
   ```bash
   pip install dbt-metricflow
   ```

2. Metrics defined in the `models/metrics.yml` file:
   ```yaml
   version: 2
   
   metrics:
     - name: total_current_balance
       type: SIMPLE
       type_params:
         measure: current_balance
       label: "Total Current Balance"
       # ... more configuration ...
   ```

3. Running with the same dbt profile:
   ```bash
   # To list metrics
   mf list metrics

   # To validate the semantic layer configuration
   mf validate-configs

   # To query metrics
   mf query --metrics total_current_balance --group-by metric_time
   ```

## Snowflake Migration (Future)

When you're ready to migrate to Snowflake, add a new target to your profile:

```yaml
cmbs_analytics:
  target: dev
  outputs:
    dev:
      # Postgres config as above
      
    snowflake:
      type: snowflake
      account: your_account
      user: "{{ env_var('SNOWFLAKE_USER') }}"
      password: "{{ env_var('SNOWFLAKE_PASSWORD') }}"
      role: "{{ env_var('SNOWFLAKE_ROLE', 'TRANSFORMER') }}"
      database: "{{ env_var('SNOWFLAKE_DATABASE', 'CMBS_ANALYTICS') }}"
      warehouse: "{{ env_var('SNOWFLAKE_WAREHOUSE', 'TRANSFORMING') }}"
      schema: "{{ env_var('SNOWFLAKE_SCHEMA', 'DBT_MODELS') }}"
      threads: 8
      client_session_keep_alive: True
```

To use the Snowflake target, you would run:

```bash
dbt run --target snowflake
```

The same profile will be used by MetricFlow:

```bash
mf --profile cmbs_analytics --target snowflake list metrics
``` 