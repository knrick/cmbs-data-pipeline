import os
import sys
from typing import Optional, Dict, Any
from pyspark.sql import SparkSession
from dotenv import load_dotenv
from .logging_utils import setup_logger

# Load environment variables (for database credentials only)
load_dotenv()

# Set up logger for this module
logger = setup_logger(__name__)

def get_spark_config() -> Dict[str, Any]:
    """Get Spark configuration with optimized settings for CMBS data processing."""
    return {
        # Memory and CPU configurations
        "spark.driver.memory": "4g",
        "spark.sql.shuffle.partitions": "8",
        "spark.default.parallelism": "8",
        
        # Performance optimizations
        "spark.sql.execution.arrow.enabled": "true",
        "spark.driver.extraJavaOptions": "-XX:+UseG1GC",
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.adaptive.skewJoin.enabled": "true",
        "spark.sql.files.maxPartitionBytes": "134217728",  # 128MB
        "spark.sql.broadcastTimeout": "600",  # 10 minutes
        
        # File format optimizations
        "spark.sql.parquet.compression.codec": "snappy",
        "spark.sql.parquet.mergeSchema": "true",
        "spark.sql.parquet.filterPushdown": "true",
        
        # Memory management
        "spark.memory.offHeap.enabled": "true",
        "spark.memory.offHeap.size": "2g",
        "spark.cleaner.periodicGC.interval": "15min",
        
        # Timezone and encoding
        "spark.sql.session.timeZone": "UTC",
        "spark.sql.files.openCostInBytes": "134217728",  # 128MB
        "spark.sql.files.ignoreMissingFiles": "true",
        
        # Windows-specific configurations
        "spark.sql.warehouse.dir": f"file:///{os.path.abspath('spark-warehouse')}" if sys.platform.startswith('win') else None,
        "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version": "2" if sys.platform.startswith('win') else None,
        "spark.hadoop.fs.defaultFS": "file:///" if sys.platform.startswith('win') else None,
        "spark.hadoop.fs.permissions.umask-mode": "000" if sys.platform.startswith('win') else None,
    }

def create_or_get_spark_session(
    app_name: str = "CMBS-Data-Pipeline",
    jdbc_jar_path: Optional[str] = "utils/postgresql-42.7.5.jar",
    xml_package: bool = False
) -> SparkSession:
    """
    Create or get an existing SparkSession with proper configurations.
    
    Args:
        app_name: Name of the Spark application
        jdbc_jar_path: Path to PostgreSQL JDBC driver jar file
        xml_package: Whether to include Spark XML package
    
    Returns:
        Configured SparkSession instance
    """
    # Get base configurations
    configs = get_spark_config()
    
    # Initialize builder
    builder = SparkSession.builder.appName(app_name)
    
    # Set master to local if not running in cluster mode
    if not any(conf.startswith('spark.master') for conf in configs):
        builder = builder.master('local[*]')
    
    # Add configurations
    for key, value in configs.items():
        if value is not None:  # Skip None values (conditional configs)
            builder = builder.config(key, value)
    
    # Add JDBC driver if specified
    if jdbc_jar_path and os.path.exists(jdbc_jar_path):
        builder = builder.config("spark.jars", jdbc_jar_path)
        logger.info(f"Added JDBC driver from {jdbc_jar_path}")
    
    # Add XML package if requested
    if xml_package:
        builder = builder.config("spark.jars.packages", "com.databricks:spark-xml_2.12:0.16.0")
        logger.info("Added Spark XML package")
    
    # Get or create session
    spark = builder.getOrCreate()
    
    # Log configuration
    logger.info(f"Created Spark session for {app_name}")
    
    return spark

# Example usage
if __name__ == "__main__":
    # Create session for XML processing
    spark_xml = create_or_get_spark_session(
        app_name="CMBS-XML-Processor",
        xml_package=True
    )
    
    # Create session for Parquet processing
    spark_parquet = create_or_get_spark_session(
        app_name="CMBS-Parquet-Processor"
    ) 