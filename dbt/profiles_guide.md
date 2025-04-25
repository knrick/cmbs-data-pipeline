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