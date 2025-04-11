import os
import logging
from typing import Dict, Any, List
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import *

from .logging_utils import setup_logger

# Set up logger for this module
logger = setup_logger(__name__)

def save_dataframe_as_parquet(df: DataFrame, output_path: str, partition_by: List[str] = None, mode: str = "append"):
    """Save DataFrame as Parquet with proper error handling."""
    logger.info(f"Saving DataFrame to {output_path}")
    try:
        # Ensure the directory exists
        os.makedirs(output_path, exist_ok=True)
        
        # Configure writer
        writer = df.write.mode(mode).option("compression", "snappy")
        
        # Add partitioning if specified
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        
        # Save the DataFrame
        writer.parquet(output_path)
        logger.info("DataFrame saved successfully")
            
    except Exception as e:
        raise Exception(f"Error saving DataFrame to {output_path}: {str(e)}")

def find_parquet_files(data_dir: str) -> Dict[str, List[Dict[str, Any]]]:
    """Find all parquet files for loans and properties."""
    parquet_files = {
        'loans': [],
        'properties': []
    }
    
    # Walk through company and trust directories
    for company_name in os.listdir(data_dir):
        company_dir = os.path.join(data_dir, company_name)
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

def read_parquet_files(spark: SparkSession, parquet_files: Dict[str, List[Dict[str, Any]]]) -> Dict[str, DataFrame]:
    """Read parquet files and return Spark DataFrames for each table."""
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
                df = spark.read.parquet(path)
                logger.info(f"Read {df.count()} rows with columns: {df.columns}")
                
                # Add company and trust if not present
                if 'company' not in df.columns:
                    df = df.withColumn('company', F.lit(file_info['company']))
                if 'trust' not in df.columns:
                    df = df.withColumn('trust', F.lit(file_info['trust']))
                
                dfs.append(df)
                
            except Exception as e:
                logger.error(f"Error processing {path}: {str(e)}")
        
        if dfs:
            # Combine all DataFrames for this table
            combined_df = dfs[0]
            for df in dfs[1:]:
                combined_df = combined_df.unionByName(df)
            logger.info(f"Combined DataFrame has {combined_df.count()} rows")
            dataframes[table_name] = combined_df
            
    return dataframes 