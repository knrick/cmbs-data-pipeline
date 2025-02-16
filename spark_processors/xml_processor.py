import os
import json
from datetime import datetime
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import *

from .schema_manager import SchemaManager
from .data_profiler import DataProfiler
from .logging_utils import setup_logger

# Set up logger for this module
logger = setup_logger(__name__)

class XMLProcessor:
    def __init__(self, spark=None, output_dir="./processed_data", partition_by_date=True):
        """Initialize XMLProcessor with optional Spark session and output directory."""
        logger.info("Initializing XMLProcessor")
        self.spark = spark or self._create_spark_session()
        self.schema_manager = SchemaManager()
        self.data_profiler = DataProfiler()
        self.output_dir = output_dir
        self.partition_by_date = partition_by_date
        # Create output directories
        logger.info(f"Creating output directories in {output_dir}")
        os.makedirs(output_dir, exist_ok=True)
        os.makedirs(os.path.join(output_dir, "parquet"), exist_ok=True)
        os.makedirs(os.path.join(output_dir, "audit_logs"), exist_ok=True)
        os.makedirs(os.path.join(output_dir, "summaries"), exist_ok=True)
        
    def _create_spark_session(self):
        """Create a local Spark session for testing."""
        logger.info("Creating new Spark session")
        import sys
        python_path = sys.executable
        
        # Set Hadoop home and disable native libraries for Windows
        os.environ['HADOOP_HOME'] = os.path.abspath(os.path.dirname(__file__))
        os.environ['HADOOP_OPTS'] = '-Djava.library.path=""'
        
        builder = SparkSession.builder \
            .appName("CMBS-XML-Processor") \
            .master("local[*]") \
            .config("spark.driver.memory", "4g") \
            .config("spark.sql.shuffle.partitions", "8") \
            .config("spark.default.parallelism", "8") \
            .config("spark.sql.execution.arrow.enabled", "true") \
            .config("spark.driver.extraJavaOptions", "-XX:+UseG1GC") \
            .config("spark.pyspark.python", python_path) \
            .config("spark.pyspark.driver.python", python_path) \
            .config("spark.jars.packages", "com.databricks:spark-xml_2.12:0.16.0")
        
        # Windows-specific configurations
        if sys.platform.startswith('win'):
            logger.info("Applying Windows-specific Spark configurations")
            builder = builder \
                .config("spark.sql.warehouse.dir", "file:///" + os.path.abspath("spark-warehouse")) \
                .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2") \
                .config("spark.hadoop.fs.defaultFS", "file:///") \
                .config("spark.hadoop.fs.permissions.umask-mode", "000")
        
        return builder.getOrCreate()
    
    def validate_boolean_values(self, df, column):
        """Validate that boolean columns contain only allowed values."""
        logger.info(f"Validating boolean values for column: {column}")
        allowed_values = ['true', 'false', '1', '0', None]
        invalid = df.filter(
            ~F.lower(F.col(column)).isin(allowed_values) & 
            ~F.col(column).isNull()
        )
        
        if invalid.count() > 0:
            invalid_values = [row[column] for row in invalid.collect()]
            logger.error(f"Invalid boolean values found in column {column}: {invalid_values}")
            raise ValueError(
                f"Column {column} contains invalid boolean values: {invalid_values}"
            )
    
    def validate_required_columns(self, df):
        """Validate that required columns are not null."""
        logger.info("Validating required columns")
        required_cols = ["assetNumber", "reportingPeriodEndDate", "company"]
        
        for col in required_cols:
            null_count = df.filter(F.col(col).isNull()).count()
            if null_count > 0:
                logger.error(f"Required column {col} contains {null_count} null values")
                raise ValueError(f"Required column {col} contains {null_count} null values")
    
    def save_audit_log(self, trust_summary, company_name, trust_name):
        """Save audit information in JSON format."""
        logger.info(f"Saving audit log for {company_name}/{trust_name}")
        audit_file = os.path.join(
            self.output_dir,
            "audit_logs",
            f"audit_{company_name}_{trust_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        )
        
        try:
            with open(audit_file, 'w') as f:
                json.dump(trust_summary, f, indent=2)
            logger.info(f"Audit log saved to {audit_file}")
        except Exception as e:
            logger.error(f"Error saving audit log: {str(e)}")
            raise

    def create_empty_dataframe(self, schema_type="loan"):
        """Create an empty DataFrame with the specified schema."""
        logger.info(f"Creating empty DataFrame with schema type: {schema_type}")
        schema = self.schema_manager.get_spark_schema(schema_type)
        return self.spark.createDataFrame([], schema)
    
    def _save_dataframe(self, df, output_path):
        """Save DataFrame with proper error handling."""
        logger.info(f"Saving DataFrame to {output_path}")
        try:
            # Ensure the directory exists
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            
            # Save the DataFrame
            df.write \
                .mode("append") \
                .option("compression", "snappy") \
                .parquet(output_path)
            logger.info("DataFrame saved successfully")
                
        except Exception as e:
            raise Exception(f"Error saving DataFrame to {output_path}: {str(e)}")
    
    def process_directory(self, input_dir):
        """Process XML files by trust, processing each trust folder separately."""
        # Get list of trust folders
        trust_folders = []
        for company_folder in os.listdir(input_dir):
            company_path = os.path.join(input_dir, company_folder)
            if os.path.isdir(company_path):
                for trust_folder in os.listdir(company_path):
                    trust_path = os.path.join(company_path, trust_folder)
                    if os.path.isdir(trust_path):
                        trust_folders.append((company_folder, trust_folder, trust_path))

        # Process trusts in parallel using Spark's built-in parallelism
        all_audit_logs = []
        
        # Create a broadcast variable for the output directory
        output_dir_bc = self.spark.sparkContext.broadcast(self.output_dir)
        
        for company_name, trust_name, trust_path in trust_folders:
            logger.info(f"\nProcessing trust: {company_name} - {trust_name}")
            
            # Get XML files in this trust folder
            xml_files = [f for f in os.listdir(trust_path) if f.endswith('.xml')]
            if not xml_files:
                logger.info(f"No XML files found in trust folder: {trust_path}")
                continue

            try:
                # Read all XML files in the trust directory at once
                df = self.spark.read \
                    .format("com.databricks.spark.xml") \
                    .option("rowTag", "assets") \
                    .option("rootTag", "assetData") \
                    .option("inferSchema", "false") \
                    .option("recursiveFileLookup", "true") \
                    .load(os.path.join(trust_path, "*.xml"))
                
                # Add company, trust, and source file columns
                df = df.withColumn("company", F.lit(company_name)) \
                     .withColumn("trust", F.lit(trust_name)) \
                     .withColumn("source_file", F.input_file_name())
                
                # Use a single partition for better efficiency
                df = df.coalesce(1)
        
                # Extract properties and loans using Spark's built-in parallelism
                properties_df = None
                if df.select("property.*").columns:  # Check if property exists and has fields
                    properties_df = df.select(
                        "assetNumber",
                        "assetTypeNumber",
                        "assetAddedIndicator",
                        "reportingPeriodEndDate",
                        "property.*",
                        "company",
                        "trust",
                        "source_file"
                    ).where(F.col("property").isNotNull()).drop("property")
            
                    # Get the unique reporting dates for each source file
                    file_dates = df.select("source_file", "reportingPeriodEndDate") \
                        .dropDuplicates() \
                        .filter(F.col("reportingPeriodEndDate").isNotNull()) \
                        .collect()
                    
                    # Create a mapping of file to its unique date
                    file_date_map = {row["source_file"]: row["reportingPeriodEndDate"] for row in file_dates}
                    
                    # Add reportingPeriodEndDate and imputation indicator to properties
                    properties_df = properties_df.withColumn(
                        "is_reporting_date_imputed",
                        F.col("reportingPeriodEndDate").isNull()  # True if date was null before imputation
                    ).withColumn(
                        "reportingPeriodEndDate",
                        F.coalesce(
                            F.col("reportingPeriodEndDate"),  # Keep original if exists
                            F.element_at(
                                F.create_map([F.lit(x) for x in sum([(k, v) for k, v in file_date_map.items()], ())]),
                                F.col("source_file")
                            )
                        )
                    )
                
                # Create loans dataframe
                loans_df = df.where(F.col("reportingPeriodEndDate").isNotNull()) \
                            .drop("property")

                # Check for existing records in parquet files and remove duplicates
                loans_output_path = os.path.join(
                    output_dir_bc.value, "parquet", company_name, trust_name, "loans"
                )
                properties_output_path = os.path.join(
                    output_dir_bc.value, "parquet", company_name, trust_name, "properties"
                )

                # Remove existing loan records
                if os.path.exists(loans_output_path):
                    logger.info(f"Checking for existing loan records in {loans_output_path}")
                    # Create two versions of existing loans with different date formats
                    existing_loans_base = self.spark.read.parquet(loans_output_path) \
                        .select(
                            F.col("assetNumber").cast("string").alias("assetNumber"),
                            F.col("reportingPeriodEndDate").alias("reportingPeriodEndDate")
                        )
                    
                    # Version 1: yyyy-MM-dd
                    existing_loans_iso = existing_loans_base \
                        .select(
                            "assetNumber",
                            F.date_format("reportingPeriodEndDate", "yyyy-MM-dd").alias("reportingPeriodEndDate")
                        )
                    
                    # Version 2: MM-dd-yyyy
                    existing_loans_us = existing_loans_base \
                        .select(
                            "assetNumber",
                            F.date_format("reportingPeriodEndDate", "MM-dd-yyyy").alias("reportingPeriodEndDate")
                        )
                    
                    # Combine both versions
                    existing_loans = existing_loans_iso.union(existing_loans_us)
                    
                    # Anti-join to keep only new records
                    original_count = loans_df.count()
                    loans_df = loans_df.join(
                        existing_loans,
                        ["assetNumber", "reportingPeriodEndDate"],
                        "leftanti"
                    )
                    new_count = loans_df.count()
                    if original_count > new_count:
                        logger.info(f"Filtered out {original_count - new_count} existing loan records")
                    
                    if new_count == 0:
                        logger.info("No new loan records to process")
                        loans_df = None

                # Remove existing property records
                if properties_df is not None and os.path.exists(properties_output_path):
                    logger.info(f"Checking for existing property records in {properties_output_path}")
                    # Create two versions of existing properties with different date formats
                    existing_properties_base = self.spark.read.parquet(properties_output_path) \
                        .select(
                            F.col("assetNumber").cast("string").alias("assetNumber"),
                            F.col("reportingPeriodEndDate").alias("reportingPeriodEndDate")
                        )
                    
                    # Version 1: yyyy-MM-dd
                    existing_properties_iso = existing_properties_base \
                        .select(
                            "assetNumber",
                            F.date_format("reportingPeriodEndDate", "yyyy-MM-dd").alias("reportingPeriodEndDate")
                        )
                    
                    # Version 2: MM-dd-yyyy
                    existing_properties_us = existing_properties_base \
                        .select(
                            "assetNumber",
                            F.date_format("reportingPeriodEndDate", "MM-dd-yyyy").alias("reportingPeriodEndDate")
                        )
                    
                    # Combine both versions
                    existing_properties = existing_properties_iso.union(existing_properties_us)
                    
                    # Anti-join to keep only new records
                    original_count = properties_df.count()
                    properties_df = properties_df.join(
                        existing_properties,
                        ["assetNumber", "reportingPeriodEndDate"],
                        "leftanti"
                    )
                    new_count = properties_df.count()
                    if original_count > new_count:
                        logger.info(f"Filtered out {original_count - new_count} existing property records")
                    
                    if new_count == 0:
                        logger.info("No new property records to process")
                        properties_df = None

                # Skip processing if no new records
                if loans_df is None and properties_df is None:
                    logger.info(f"No new records to process for trust: {trust_name}")
                    continue

                # Prepare both DataFrames for parallel validation
                validation_dfs = []
                validation_configs = []
                
                if properties_df is not None:
                    # Add type column for identification
                    properties_df = properties_df.withColumn("_validation_type", F.lit("property"))
                    validation_dfs.append(properties_df)
                    validation_configs.append("property")
                
                # Add type column for identification
                loans_df = loans_df.withColumn("_validation_type", F.lit("loan"))
                validation_dfs.append(loans_df)
                validation_configs.append("loan")
                
                # Union all DataFrames to validate them in a single pass
                combined_df = validation_dfs[0]
                for df in validation_dfs[1:]:
                    combined_df = combined_df.unionByName(df, allowMissingColumns=True)
                
                # Validate all data in parallel
                validated_dfs = self.schema_manager.validate_dataframe_schema(combined_df, validation_configs)
                
                # Split back into individual DataFrames
                if properties_df is not None:
                    properties_df = validated_dfs["property"]
                loans_df = validated_dfs["loan"]
                
                # Use Spark's built-in parallelism for data quality checks
                duplicates = self.data_profiler.check_duplicates(loans_df)
                suspicious = self.data_profiler.update_profile(loans_df)
        
                if duplicates:
                    logger.info(f"Found duplicates in loan data")
                if suspicious:
                    logger.info(f"Found suspicious values in these columns: {suspicious.keys()}")
                
                # Remove duplicates from loans, keeping the latest version based on processing order
                loans_count_before = loans_df.count()
                loans_df = loans_df.dropDuplicates(["assetNumber", "reportingPeriodEndDate"])
                loans_count_after = loans_df.count()
                if loans_count_before > loans_count_after:
                    logger.info(f"Removed {loans_count_before - loans_count_after} duplicate loan records")
                
                # Save trust results
                if loans_df is not None:
                    os.makedirs(loans_output_path, exist_ok=True)
                    
                    logger.info(f"Saving {loans_df.count()} loan records with {len(loans_df.columns)} columns for {trust_name}")
                    if self.partition_by_date:
                        loans_df.write \
                            .mode("append") \
                            .option("compression", "snappy") \
                            .partitionBy("reportingPeriodEndDate") \
                            .parquet(loans_output_path)
                    else:
                        loans_df.write \
                            .mode("append") \
                            .option("compression", "snappy") \
                            .parquet(loans_output_path)
                
                if properties_df is not None:
                    os.makedirs(properties_output_path, exist_ok=True)
                    
                    logger.info(f"Saving {properties_df.count()} property records with {len(properties_df.columns)} columns for {trust_name}")
                    properties_df.write \
                        .mode("append") \
                        .option("compression", "snappy") \
                        .parquet(properties_output_path)
                
                # Create trust summary
                trust_summary = {
                    "company": company_name,
                    "trust": trust_name,
                    "processing_timestamp": datetime.now().isoformat(),
                    "files_processed": len(xml_files),
                    "files_succeeded": len(xml_files),  # All files processed together
                    "files_failed": 0,
                    "total_loans": loans_df.count() if loans_df is not None else 0,
                    "total_properties": properties_df.count() if properties_df is not None else 0,
                    "data_quality": {
                        "loans": {
                        "duplicates": duplicates,
                        "suspicious_values": suspicious
                        }
                    }
                }
                
                # Save trust summary and audit log
                trust_summary_path = os.path.join(
                    output_dir_bc.value, "summaries", company_name, trust_name, "processing_summary.json"
                )
                os.makedirs(os.path.dirname(trust_summary_path), exist_ok=True)
                with open(trust_summary_path, 'w') as f:
                    json.dump(trust_summary, f, indent=2)
                
                self.save_audit_log(trust_summary, company_name, trust_name)
                all_audit_logs.append(trust_summary)
                
                logger.info(f"Completed processing trust: {company_name} - {trust_name}")
                logger.info(f"Processed {trust_summary['files_succeeded']} files successfully")
                logger.info(f"Total loans: {trust_summary['total_loans']}, Total properties: {trust_summary['total_properties']}")
                
            except Exception as e:
                error_msg = str(e)
                logger.error(f"Error processing trust {trust_name}: {error_msg}")
                error_summary = {
                    "company": company_name,
                    "trust": trust_name,
                    "processing_timestamp": datetime.now().isoformat(),
                    "files_processed": len(xml_files),
                    "files_succeeded": 0,
                    "files_failed": len(xml_files),
                    "error": error_msg
                }
                all_audit_logs.append(error_summary)
        
        # Cleanup broadcast variable
        output_dir_bc.unpersist()
        
        # Create overall summary
        final_summary = {
            "processing_timestamp": datetime.now().isoformat(),
            "trusts_processed": len(trust_folders),
            "total_files_processed": sum(log.get("files_processed", 0) for log in all_audit_logs),
            "total_files_succeeded": sum(log.get("files_succeeded", 0) for log in all_audit_logs),
            "total_files_failed": sum(log.get("files_failed", 0) for log in all_audit_logs),
            "audit_logs": all_audit_logs
        }
        
        # Save final summary
        with open(os.path.join(self.output_dir, "processing_summary.json"), 'w') as f:
            json.dump(final_summary, f, indent=2)
        
        return final_summary

def main():
    # Initialize Spark with XML package
    spark = SparkSession.builder \
        .appName("CMBS_XML_Processor") \
        .config("spark.sql.files.maxPartitionBytes", "134217728") \
        .config("spark.jars.packages", "com.databricks:spark-xml_2.12:0.16.0") \
        .getOrCreate()
    
    # Create processor
    processor = XMLProcessor(spark)
    
    # Process all XML files
    summary = processor.process_directory("./data")
    logger.info(json.dumps(summary, indent=2))
    
    spark.stop()

if __name__ == "__main__":
    main() 