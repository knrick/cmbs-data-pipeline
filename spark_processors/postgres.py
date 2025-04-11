import os
import re
import json
from typing import Dict, Any, List
import logging
from pathlib import Path
import platform
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
    
    if 'reportingPeriodEndDate' in schema['columns']:
        sql_parts.append(f"\n-- Add index for reporting date")
        sql_parts.append(f"CREATE INDEX idx_{table_name}_reporting_date ON {table_name}(reporting_period_end_date);")
    
    return '\n'.join(sql_parts)

class PostgresConnector:
    def __init__(self, dbname='cmbs_data', user='postgres', password='postgres', host='localhost', port=5432):
        """Initialize database connection parameters."""
        self.params = {
            'dbname': dbname,
            'user': user,
            'password': password,
            'host': host,
            'port': port
        }
        
        # JDBC URL for Spark
        self.jdbc_url = f"jdbc:postgresql://{host}:{port}/{dbname}"
        self.connection_properties = {
            "user": user,
            "password": password,
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
        return self.execute_query_dict(query)
    
    def get_row_count(self, table_name: str) -> int:
        """Get the number of rows in a table."""
        conn = psycopg2.connect(**self.params)
        try:
            with conn.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) as count FROM {table_name}")
                return cur.fetchone()[0]
        finally:
            conn.close()

class ParquetLoader:
    def __init__(self, spark=None, data_dir="./processed_data/parquet"):
        """Initialize the loader with Spark session and data directory path."""
        self.spark = spark or self._create_spark_session()
        self.data_dir = data_dir
        self.db = PostgresConnector()
        self.is_windows = platform.system() == 'Windows'

    def _create_spark_session(self):
        """Create a local Spark session with proper configurations."""
        logger.info("Creating new Spark session")
        import sys
        python_path = sys.executable
        
        builder = SparkSession.builder \
            .appName("CMBS-Parquet-Loader") \
            .master("local[*]") \
            .config("spark.driver.memory", "4g") \
            .config("spark.sql.shuffle.partitions", "8") \
            .config("spark.default.parallelism", "8") \
            .config("spark.sql.execution.arrow.enabled", "true") \
            .config("spark.driver.extraJavaOptions", "-XX:+UseG1GC") \
            .config("spark.pyspark.python", python_path) \
            .config("spark.pyspark.driver.python", python_path) \
            .config("spark.jars", "utils/postgresql-42.7.5.jar")
        
        # Windows-specific configurations
        if sys.platform.startswith('win'):
            logger.info("Applying Windows-specific Spark configurations")
            builder = builder \
                .config("spark.sql.warehouse.dir", "file:///" + os.path.abspath("spark-warehouse")) \
                .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2") \
                .config("spark.hadoop.fs.defaultFS", "file:///") \
                .config("spark.hadoop.fs.permissions.umask-mode", "000")
        
        return builder.getOrCreate()

    def find_parquet_files(self) -> Dict[str, List[Dict[str, Any]]]:
        """Find all parquet files for loans and properties."""
        parquet_files = {
            'loans': [],
            'properties': []
        }
        
        # Walk through company and trust directories
        for company_name in os.listdir(self.data_dir):
            company_dir = os.path.join(self.data_dir, company_name)
            if not os.path.isdir(company_dir):
                continue
            
            for trust_name in os.listdir(company_dir):
                trust_dir = os.path.join(company_dir, trust_name)
                if not os.path.isdir(trust_dir):
                    continue
                
                logger.info(f"Processing {company_name} - {trust_name}")
                
                # Handle loans (partitioned by date)
                loans_dir = os.path.join(trust_dir, 'loans')
                if os.path.exists(loans_dir):
                    # Check for SUCCESS file
                    if not os.path.exists(os.path.join(loans_dir, '_SUCCESS')):
                        logger.warning(f"No SUCCESS file found in {loans_dir}")
                        continue
                    
                    parquet_files['loans'].append({
                        'path': loans_dir,
                        'company': company_name,
                        'trust': trust_name
                    })
                
                # Handle properties (not partitioned)
                properties_dir = os.path.join(trust_dir, 'properties')
                if os.path.exists(properties_dir):
                    # Check for SUCCESS file
                    if not os.path.exists(os.path.join(properties_dir, '_SUCCESS')):
                        logger.warning(f"No SUCCESS file found in {properties_dir}")
                        continue
                    
                    parquet_files['properties'].append({
                        'path': properties_dir,
                        'company': company_name,
                        'trust': trust_name
                    })
                    
        return parquet_files

    def get_column_mapping(self, df: DataFrame, table_name: str) -> Dict[str, str]:
        """Create a mapping between Spark DataFrame columns and PostgreSQL columns."""
        # Get PostgreSQL table columns
        table_info = self.db.get_table_info(table_name)
        pg_columns = [col['column_name'] for col in table_info]
        
        # Create mapping dictionary
        column_map = {}
        unmapped_pg_cols = []
        unmapped_df_cols = []
        
        # First, try exact matches (case-insensitive)
        for df_col in df.columns:
            pg_col = df_col.lower()
            if pg_col in pg_columns:
                column_map[df_col] = pg_col
                pg_columns.remove(pg_col)
        
        # Then try standardized names
        remaining_df_cols = [col for col in df.columns if col not in column_map]
        for df_col in remaining_df_cols:
            std_name = standardize_column_name(df_col)
            if std_name in pg_columns:
                column_map[df_col] = std_name
                pg_columns.remove(std_name)
            else:
                unmapped_df_cols.append(df_col)
        
        unmapped_pg_cols = pg_columns
        
        # Log mapping results
        logger.info(f"\nColumn mapping for {table_name}:")
        logger.info("Mapped columns:")
        for df_col, pg_col in sorted(column_map.items()):
            logger.info(f"  {df_col} -> {pg_col}")
        
        if unmapped_df_cols:
            logger.warning("Unmapped DataFrame columns:")
            for col in sorted(unmapped_df_cols):
                logger.warning(f"  {col}")
        
        if unmapped_pg_cols:
            logger.warning("Unmapped PostgreSQL columns:")
            for col in sorted(unmapped_pg_cols):
                logger.warning(f"  {col}")
        
        return column_map
    
    def standardize_dataframe(self, df: DataFrame, table_name: str) -> DataFrame:
        """Standardize Spark DataFrame to match PostgreSQL schema."""
        # Get column mapping
        column_map = self.get_column_mapping(df, table_name)
        
        # Rename columns
        for old_col, new_col in column_map.items():
            df = df.withColumnRenamed(old_col, new_col)
        
        # Get PostgreSQL table columns
        table_info = self.db.get_table_info(table_name)
        pg_columns = [col['column_name'] for col in table_info]
        
        # Add missing columns with NULL values
        for col in pg_columns:
            if col not in df.columns:
                df = df.withColumn(col, lit(None))
        
        # Select columns in the correct order
        return df.select(pg_columns)
    
    def get_dataframes(self) -> Dict[str, DataFrame]:
        """Read parquet files and return Spark DataFrames for each table."""
        parquet_files = self.find_parquet_files()
        dataframes = {}
        
        for table_name, file_infos in parquet_files.items():
            if not file_infos:
                logger.warning(f"No parquet files found for {table_name}")
                continue
                
            logger.info(f"Processing {len(file_infos)} directories for {table_name}")
            
            # List to store DataFrames for each trust
            dfs = []
            
            for file_info in file_infos:
                try:
                    path = file_info['path']
                    logger.info(f"Reading from {path}")
                    
                    # Read parquet data
                    df = self.spark.read.parquet(path)
                    logger.info(f"Read {df.count()} rows with columns: {df.columns}")
                    
                    # Add company and trust if not present
                    if 'company' not in df.columns:
                        df = df.withColumn('company', lit(file_info['company']))
                    if 'trust' not in df.columns:
                        df = df.withColumn('trust', lit(file_info['trust']))
                    
                    dfs.append(df)
                    
                except Exception as e:
                    logger.error(f"Error processing {path}: {str(e)}")
            
            if dfs:
                # Combine all DataFrames for this table
                combined_df = dfs[0]
                for df in dfs[1:]:
                    combined_df = combined_df.unionByName(df)
                logger.info(f"Combined DataFrame has {combined_df.count()} rows")
                
                # Standardize the combined DataFrame
                standardized_df = self.standardize_dataframe(combined_df, table_name)
                dataframes[table_name] = standardized_df
                
        return dataframes
    
    def load_dataframes_to_postgres(self, dataframes: Dict[str, DataFrame]):
        """Load Spark DataFrames into PostgreSQL tables."""
        for table_name, df in dataframes.items():
            try:
                logger.info(f"Loading {df.count()} rows into {table_name}")
                
                # Write DataFrame to PostgreSQL
                df.write \
                    .jdbc(url=self.db.jdbc_url,
                          table=table_name,
                          mode="append",
                          properties=self.db.connection_properties)
                
                logger.info(f"Successfully loaded data into {table_name}")
            except Exception as e:
                logger.error(f"Error loading data into {table_name}: {str(e)}")
    
    def verify_data_load(self):
        """Verify that data was loaded correctly."""
        for table in ['loans', 'properties']:
            count = self.db.get_row_count(table)
            logger.info(f"Total rows in {table}: {count}")
            
            if count > 0:
                # Show distribution by company and trust
                query = f"""
                SELECT company, trust, COUNT(*) as row_count
                FROM {table}
                GROUP BY company, trust
                ORDER BY row_count DESC;
                """
                distribution = self.db.execute_query_dict(query)
                logger.info(f"\nData distribution in {table}:")
                logger.info(distribution)

# Example usage
if __name__ == "__main__":
    # Create Spark session
    spark = SparkSession.builder \
        .appName("CMBS Data Loader") \
        .config("spark.jars", "postgresql-42.2.23.jar") \
        .getOrCreate()
    
    try:
        # Create a loader instance
        loader = ParquetLoader(spark)
        
        # Get DataFrames
        dataframes = loader.get_dataframes()
        
        # Load data into PostgreSQL
        loader.load_dataframes_to_postgres(dataframes)
        
        # Verify the load
        loader.verify_data_load()
        
    finally:
        spark.stop() 