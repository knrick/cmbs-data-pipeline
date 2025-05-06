import os
import re
import json
from typing import Dict, Any, List
from datetime import datetime
from functools import reduce
import logging
from pathlib import Path
import platform
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import *
from pyspark.sql.functions import lit
from .logging_utils import setup_logger

# Set up logger for this module
logger = setup_logger(__name__)

def get_postgres_type(field_def: Dict[str, Any]) -> str:
    """Convert JSON schema type to PostgreSQL type."""
    field_type = field_def['type']
    
    if field_type == 'string':
        if field_def.get('size'):
            if field_type == 'char':
                return f"CHAR({field_def['size']})"
            return f"VARCHAR({field_def['size']})"
        return "TEXT"
    
    elif field_type == 'date':
        return "DATE"
    
    elif field_type == 'number':
        whole = field_def.get('whole', 0)
        decimal = field_def.get('decimal', 0)
        return f"NUMERIC({whole + decimal},{decimal})"
    
    elif field_type == 'bool':
        return "BOOLEAN"
    
    elif field_type == 'UInt8':
        return "SMALLINT"
    
    elif field_type == 'UInt16':
        return "INTEGER"
    
    elif field_type == 'UInt32':
        return "BIGINT"
    
    elif field_type == 'Int16':
        return "SMALLINT"
    
    elif field_type == 'char':
        size = field_def.get('size', 1)
        return f"CHAR({size})"
    
    else:
        raise ValueError(f"Unsupported type: {field_type}")

def standardize_column_name(name: str) -> str:
    """Convert camelCase to snake_case with standardized abbreviations.
    
    Args:
        name: The camelCase string to convert
        
    Returns:
        The converted snake_case string with standardized abbreviations
    """
    # First convert camelCase to snake_case
    s1 = re.sub(r'([a-z0-9])([A-Z][a-z])', r'\1_\2', name)
    s2 = re.sub(r'([A-Z][a-z])([A-Z])', r'\1_\2', s1)
    snake_case = s2.lower()
    
    # Apply standard abbreviations
    replacements = {
        # Financial terms
        '_percentage': '_pct',
        '_amount': '_amt',
        '_number': '_num',
        '_balance': '_bal',
        '_interest': '_intr',
        '_principal': '_prin',
        '_payment': '_pmt',
        
        # Time-related terms
        '_beginning': '_begin',
        '_ending': '_end',
        '_original': '_orig',
        '_expiration': '_exp',
    }
    
    # Apply replacements
    for old, new in replacements.items():
        snake_case = snake_case.replace(old, new)
    
    # Remove any special characters and replace with underscore
    snake_case = re.sub('[^a-z0-9_]', '_', snake_case)
    
    # Remove multiple consecutive underscores
    snake_case = re.sub('_+', '_', snake_case)
    
    # Remove leading/trailing underscores
    snake_case = snake_case.strip('_')
    
    # Ensure name doesn't exceed PostgreSQL's limit of 63 characters
    if len(snake_case) > 63:
        logger.warning(f"Column name '{snake_case}' exceeds PostgreSQL's 63 character limit and will be truncated")
        snake_case = snake_case[:63]
    
    return snake_case

def schema_to_sql(schema: Dict[str, Any], table_name: str, add_drop: bool = False) -> str:
    """Convert a JSON schema to a PostgreSQL CREATE TABLE statement."""
    # Start building the SQL string
    sql_parts = [f"-- Create {table_name} table"]
    if add_drop:
        sql_parts.append(f"DROP TABLE IF EXISTS {table_name};")
    sql_parts.append(f"CREATE TABLE {table_name} (")
    
    # Track columns for primary key
    pk_columns = []
    
    # Process each column
    columns = []
    for col_name, col_def in schema['columns'].items():
        # Convert camelCase to snake_case using the utility function
        pg_name = standardize_column_name(col_name)
        
        # Get the PostgreSQL type
        pg_type = get_postgres_type(col_def)
        
        # Build the column definition
        parts = [f"    {pg_name} {pg_type}"]
        
        # Add NOT NULL if required
        if not col_def.get('nullable', True):
            parts.append("NOT NULL,")
            # Track primary key columns (assuming non-nullable columns are part of PK)
            pk_columns.append(pg_name)
        else:
            parts.append("NULL,")
        
        # Add description as a comment
        if 'description' in col_def:
            comment = col_def['description'].replace("'", "''")  # Escape single quotes
            parts.append(f"-- {comment}")
        
        columns.append(' '.join(parts))
    
    # Add all column definitions
    sql_parts.append('\n'.join(columns))
    
    # Add primary key if we have non-nullable columns
    if pk_columns:
        sql_parts.append(f"\n    PRIMARY KEY ({', '.join(pk_columns)})")
    
    # Close the create table statement
    sql_parts.append(");")
    
    # Add indexes for common query patterns
    if 'company' in schema['columns'] and 'trust' in schema['columns']:
        sql_parts.append(f"\n-- Add index for company and trust")
        sql_parts.append(f"CREATE INDEX idx_{table_name}_company_trust ON {table_name}(company, trust);")
    
    # Add index for assetNumber and propertyNumber
    if 'assetNumber' in schema['columns'] and 'propertyNumber' in schema['columns']:
        sql_parts.append(f"\n-- Add index for asset_num and property_num")
        sql_parts.append(f"CREATE INDEX idx_{table_name}_asset_property ON {table_name}(asset_num, property_num);")
    
    return '\n'.join(sql_parts)

class PostgresConnector:
    def __init__(self, dbname=None, user=None, password=None, host=None, port=None):
        """Initialize database connection parameters from environment variables or arguments."""
        # Load from environment variables with fallback to arguments
        self.params = {
            'dbname': dbname or os.getenv('POSTGRES_DB', 'cmbs_data'),
            'user': user or os.getenv('POSTGRES_USER', 'postgres'),
            'password': password or os.getenv('POSTGRES_PASSWORD', 'postgres'),
            'host': host or os.getenv('POSTGRES_HOST', 'localhost'),
            'port': port or os.getenv('POSTGRES_PORT', 5432)
        }
        
        # JDBC URL for Spark
        self.jdbc_url = f"jdbc:postgresql://{self.params['host']}:{self.params['port']}/{self.params['dbname']}"
        self.connection_properties = {
            "user": self.params['user'],
            "password": self.params['password'],
            "driver": "org.postgresql.Driver"
        }
        
    def execute_sql(self, query: str) -> None:
        """Execute a SQL query without returning results."""
        conn = psycopg2.connect(**self.params)
        try:
            with conn.cursor() as cur:
                cur.execute(query)
            conn.commit()
        finally:
            conn.close()
    
    def execute_query_spark(self, spark: SparkSession, query: str) -> DataFrame:
        """Execute a query and return results as a Spark DataFrame."""
        return spark.read.jdbc(
            url=self.jdbc_url,
            table=query,
            properties=self.connection_properties
        )
    
    def execute_query_dict(self, query: str) -> list:
        """Execute a query and return results as a list of dictionaries."""
        conn = psycopg2.connect(**self.params)
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(query)
                return cur.fetchall()
        finally:
            conn.close()
    
    def get_table_info(self, table_name: str) -> List[Dict[str, Any]]:
        """Get information about table columns."""
        query = f"""
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_name = '{table_name}'
        ORDER BY ordinal_position;
        """
        return pd.DataFrame(self.execute_query_dict(query))
    
    def get_row_count(self, table_name: str) -> int:
        """Get the number of rows in a table."""
        conn = psycopg2.connect(**self.params)
        try:
            with conn.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) as count FROM {table_name}")
                return cur.fetchone()[0]
        finally:
            conn.close()
    
    def get_column_mapping(self, df: DataFrame, schema: Dict[str, Any]) -> Dict[str, str]:
        """Create a mapping between DataFrame columns and PostgreSQL columns based on schema."""
        column_map = {}
        unmapped_df_cols = []
        
        # Get schema columns
        schema_columns = {col_name: col_def for col_name, col_def in schema['columns'].items()}
        
        # First, try exact matches (case-insensitive)
        for df_col in df.columns:
            if df_col.lower() in [col.lower() for col in schema_columns.keys()]:
                column_map[df_col] = standardize_column_name(df_col)
                continue
            
            # Try standardized name
            std_name = standardize_column_name(df_col)
            if std_name in [standardize_column_name(col) for col in schema_columns.keys()]:
                column_map[df_col] = std_name
            else:
                unmapped_df_cols.append(df_col)
        
        # Log mapping results
        logger.info(f"\nColumn mapping results:")
        logger.info("Mapped columns:")
        for df_col, pg_col in sorted(column_map.items()):
            logger.info(f"  {df_col} -> {pg_col}")
        
        if unmapped_df_cols:
            logger.warning("Unmapped DataFrame columns:")
            for col in sorted(unmapped_df_cols):
                logger.warning(f"  {col}")
        
        return column_map

    def upsert_dataframe(self, df: DataFrame, table_name: str, schema: Dict[str, Any]) -> None:
        """
        Upsert DataFrame to PostgreSQL using a temporary table approach.
        
        Args:
            df: Spark DataFrame to upsert
            table_name: Target table name
            schema: JSON schema definition for the table
        """
        logger.info(f"Upserting {df.count()} rows to {table_name}")
        
        try:
            # Get column mapping based on schema
            column_map = self.get_column_mapping(df, schema)

            # Get table info
            table_info = self.get_table_info(table_name)
            
            # Identify primary key columns from table info
            pk_columns = table_info[table_info["is_nullable"] == "NO"]["column_name"].tolist()
            
            if not pk_columns:
                raise ValueError("No primary key columns found in table")
            
            # Check for extra columns in DataFrame that aren't in schema
            extra_columns = set(df.columns) - set(schema['columns'].keys())
            if extra_columns:
                raise ValueError(f"DataFrame contains columns not in schema: {extra_columns}")
            
            # Check for missing required columns in DataFrame
            missing_required = {col_name for col_name in schema['columns'] if col_name not in pk_columns} - set(df.columns)
            if missing_required:
                raise ValueError(f"DataFrame missing required columns: {missing_required}")
            
            # Create a temporary table name
            temp_table = f"{table_name}_temp_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
            # Create temporary table with proper schema
            self.execute_sql(schema_to_sql(schema, temp_table))
            
            # Standardize DataFrame column names
            df_standardized = reduce(
                lambda acc_df, col_map: acc_df.withColumnRenamed(col_map[0], col_map[1]),
                column_map.items(),
                df
            )
            
            # Write DataFrame to temporary table
            df_standardized.write \
                .jdbc(url=self.jdbc_url,
                     table=temp_table,
                     mode="append",
                     properties=self.connection_properties)
            
            # Build the SET clause excluding primary key columns
            set_columns = [
                col_name for col_name in column_map.values()
                if col_name not in pk_columns
            ]
            
            set_clause = ', '.join(
                f"{col} = EXCLUDED.{col}"
                for col in set_columns
            )
            
            # Perform upsert using SQL
            upsert_sql = f"""
            INSERT INTO {table_name}
            SELECT t.* FROM {temp_table} t
            ON CONFLICT ({', '.join(pk_columns)})
            DO UPDATE SET
                {set_clause}
            """
            
            self.execute_sql(upsert_sql)
            
            # Drop temporary table
            self.execute_sql(f"DROP TABLE {temp_table}")
            
            logger.info(f"Successfully upserted data into {table_name}")
            
        except Exception as e:
            logger.error(f"Error upserting data into {table_name}: {str(e)}")
            raise