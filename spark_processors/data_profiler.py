"""Data profiling and validation for CMBS XML data."""

import os
import json
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window
from .logging_utils import setup_logger

# Set up logger for this module
logger = setup_logger(__name__)

class DataProfiler:
    def __init__(self, profile_dir="./data_profiles"):
        """Initialize DataProfiler with profile storage directory."""
        logger.info("Initializing DataProfiler")
        self.profile_dir = profile_dir
        os.makedirs(profile_dir, exist_ok=True)
        
        # Initialize empty historical profiles with required structure
        self.historical_profiles = {
            "duplicates": [],  # List to store duplicate records
            "columns": {}      # Dictionary to store column profiles
        }
        
        # Load historical profiles if they exist
        self.profile_path = os.path.join(profile_dir, "historical_profiles.json")
        if os.path.exists(self.profile_path):
            try:
                self._load_historical_profiles()
            except Exception as e:
                logger.error(f"Error loading historical profiles: {str(e)}")
                # Continue with empty profiles if loading fails
        
        # Thresholds for suspicious values
        self.suspicious_thresholds = {
            "null_ratio": 0.5,  # More than 50% nulls
            "unique_ratio": 0.9,  # More than 90% unique values
            "constant_ratio": 0.9,  # Same value in more than 90% of rows
            "string_length_increase": 2.0,  # String length more than 2x historical max
            "numeric_range_expansion": 1.5  # Numeric range expanded by 50%
        }
        logger.info("Initialized with default thresholds")
    
    def _load_historical_profiles(self):
        """Load historical profiles from disk."""
        try:
            if os.path.exists(self.profile_path) and os.path.getsize(self.profile_path) > 0:
                with open(self.profile_path, 'r') as f:
                    loaded_profiles = json.load(f)
                    # Ensure required structure exists
                    if "duplicates" not in loaded_profiles:
                        loaded_profiles["duplicates"] = []
                    if "columns" not in loaded_profiles:
                        loaded_profiles["columns"] = {}
                    self.historical_profiles = loaded_profiles
            else:
                self.historical_profiles = {
                    "duplicates": [],
                    "columns": {}
                }
        except json.JSONDecodeError as e:
            logger.error(f"Error decoding historical profiles JSON: {str(e)}")
            self.historical_profiles = {"duplicates": [], "columns": {}}
        except Exception as e:
            logger.error(f"Error loading historical profiles: {str(e)}")
            self.historical_profiles = {"duplicates": [], "columns": {}}
    
    def _save_historical_profiles(self):
        """Save historical profiles to JSON file."""
        try:
            # Ensure directory exists
            os.makedirs(os.path.dirname(self.profile_path), exist_ok=True)
            
            # Create a deep copy to modify
            profiles_copy = {
                "duplicates": self.historical_profiles["duplicates"],
                "columns": {}
            }
            
            # Convert datetime objects in column profiles
            for col_name, profiles in self.historical_profiles["columns"].items():
                profiles_copy["columns"][col_name] = []
                for profile in profiles:
                    profile_copy = {}
                    for key, value in profile.items():
                        if isinstance(value, datetime):
                            profile_copy[key] = value.isoformat()
                        else:
                            profile_copy[key] = value
                    profiles_copy["columns"][col_name].append(profile_copy)
            
            # Write to a temporary file first
            temp_path = self.profile_path + '.tmp'
            with open(temp_path, 'w') as f:
                json.dump(profiles_copy, f, indent=2, default=str)
            
            # Rename temporary file to actual file (atomic operation)
            os.replace(temp_path, self.profile_path)
                
        except Exception as e:
            logger.error(f"Error saving historical profiles: {str(e)}")
            # Remove temporary file if it exists
            if os.path.exists(temp_path):
                os.remove(temp_path)
            raise
    
    def validate_single_property(self, df):
        """Validate that each asset has at most one property."""
        logger.info("Validating single property per asset")
        try:
            if "property" not in df.columns:
                logger.warning("No property column found in DataFrame")
                return False
            
            property_counts = df.filter(F.col("property").isNotNull()) \
                           .groupBy("assetNumber") \
                           .count() \
                           .filter(F.col("count") > 1)
        
            multiple_properties = property_counts.count()
            if multiple_properties > 0:
                logger.error(f"Found {multiple_properties} assets with multiple properties")
                return False
            
            logger.info("Property validation passed")
            return True
            
        except Exception as e:
            logger.error(f"Error validating properties: {str(e)}")
            return False
    
    def check_duplicates(self, df):
        """Check for duplicates and identify their source."""
        logger.info("Checking for duplicates")
        
        # First check for nulls in key columns
        null_keys = df.filter(
            F.col("assetNumber").isNull() | 
            F.col("reportingPeriodEndDate").isNull()
        )
        
        if null_keys.count() > 0:
            error_msg = "Found null values in key columns"
            logger.error(error_msg)
            raise ValueError(error_msg)
        
        # Find duplicates
        dupes = df.groupBy("assetNumber", "reportingPeriodEndDate") \
                 .agg(
                     F.count("*").alias("count"),
                     F.collect_set("source_file").alias("source_files")
                 ) \
                 .filter("count > 1")
        
        if dupes.count() > 0:
            # Analyze duplicates
            duplicate_info = []
            for row in dupes.collect():
                duplicate_info.append({
                    "assetNumber": row["assetNumber"],
                    "reportingPeriodEndDate": row["reportingPeriodEndDate"].isoformat(),
                    "count": row["count"],
                    "source_files": row["source_files"],
                    "is_cross_file": len(row["source_files"]) > 1
                })
            
            # Save to historical record
            self.historical_profiles["duplicates"].append({
                "timestamp": datetime.now().isoformat(),
                "duplicates": duplicate_info
            })
            self._save_historical_profiles()
            
            return {
                "duplicate_count": dupes.count(),
                "duplicate_info": duplicate_info
            }
        
        return None
    
    def get_column_stats(self, df, column_name):
        """Get statistics for a column, handling different data types appropriately."""
        logger.info(f"Getting stats for column: {column_name}")
        
        # Get column data type
        col_type = df.schema[column_name].dataType
        
        # Cache DataFrame if it will be used multiple times
        if isinstance(col_type, (IntegerType, LongType, FloatType, DoubleType, DecimalType)):
            df = df.cache()
        
        try:
            # Combine all basic stats into a single aggregation
            agg_exprs = [
                F.count('*').alias('total_count'),
                F.count(F.when(F.col(column_name).isNull(), True)).alias('null_count'),
                F.min(column_name).alias('min_value'),
                F.max(column_name).alias('max_value')
            ]
            
            # Add type-specific aggregations
            if isinstance(col_type, (IntegerType, LongType, FloatType, DoubleType, DecimalType)):
                agg_exprs.extend([
                    F.avg(column_name).alias('mean'),
                    F.stddev(column_name).alias('stddev')
                ])
            elif isinstance(col_type, BooleanType):
                agg_exprs.extend([
                    F.sum(F.when(F.col(column_name) == True, 1).otherwise(0)).alias('true_count'),
                    F.sum(F.when(F.col(column_name) == False, 1).otherwise(0)).alias('false_count')
                ])
            
            # Execute all aggregations at once
            stats_row = df.select(*agg_exprs).first()
            
            # Build stats dictionary
            stats = {
                'total_count': stats_row['total_count'],
                'null_count': stats_row['null_count'],
                'min_value': stats_row['min_value'],
                'max_value': stats_row['max_value']
            }
            
            # Add distinct count (separate operation due to different handling for boolean)
            if isinstance(col_type, BooleanType):
                distinct_vals = df.select(column_name).distinct().collect()
                stats['distinct_count'] = len([r[0] for r in distinct_vals if r[0] is not None]) + (1 if stats['null_count'] > 0 else 0)
            else:
                stats['distinct_count'] = df.select(
                    F.countDistinct(F.coalesce(F.col(column_name).cast("string"), F.lit("NULL")))
                ).first()[0]
            
            # Add type-specific stats
            if isinstance(col_type, (IntegerType, LongType, FloatType, DoubleType, DecimalType)):
                stats.update({
                    'mean': float(stats_row['mean']) if stats_row['mean'] is not None else None,
                    'stddev': float(stats_row['stddev']) if stats_row['stddev'] is not None else None
                })
            elif isinstance(col_type, BooleanType):
                stats.update({
                    'true_count': int(stats_row['true_count']) if stats_row['true_count'] is not None else 0,
                    'false_count': int(stats_row['false_count']) if stats_row['false_count'] is not None else 0
                })
            
            return stats
        
        finally:
            # Uncache if we cached it
            if isinstance(col_type, (IntegerType, LongType, FloatType, DoubleType, DecimalType)):
                df.unpersist()
    
    def update_profile(self, df):
        """Update data profile with new DataFrame information."""
        logger.info("Updating data profile")
        suspicious_values = {}
        
        # Calculate total count once
        total_count = df.count()
        logger.info(f"Collecting aggregations for {total_count} rows")
        
        # Prepare aggregations for all columns at once
        all_agg_exprs = []
        column_configs = {}  # Store configuration for each column
        
        for field in df.schema.fields:
            col_name = field.name
            col_type = field.dataType
            
            # Basic stats for all types
            all_agg_exprs.extend([
                F.count(F.when(F.col(col_name).isNull(), True)).alias(f'{col_name}_null_count'),
                F.countDistinct(F.coalesce(F.col(col_name).cast("string"), F.lit("NULL"))).alias(f'{col_name}_distinct_count')
            ])
            
            # Type-specific aggregations
            if isinstance(col_type, (IntegerType, LongType, FloatType, DoubleType, DecimalType)):
                all_agg_exprs.extend([
                    F.min(col_name).alias(f'{col_name}_min_value'),
                    F.max(col_name).alias(f'{col_name}_max_value'),
                    F.avg(col_name).alias(f'{col_name}_mean'),
                    F.stddev(col_name).alias(f'{col_name}_stddev'),
                    F.expr(f'percentile({col_name}, 0.5)').alias(f'{col_name}_median')
                ])
            elif isinstance(col_type, DateType):
                all_agg_exprs.extend([
                    F.min(col_name).alias(f'{col_name}_earliest_date'),
                    F.max(col_name).alias(f'{col_name}_latest_date')
                ])
            elif isinstance(col_type, StringType):
                all_agg_exprs.extend([
                    F.avg(F.length(col_name)).alias(f'{col_name}_avg_length'),
                    F.max(F.length(col_name)).alias(f'{col_name}_max_length'),
                    F.expr(f'percentile(length({col_name}), 0.5)').alias(f'{col_name}_median_length'),
                    F.count(F.when(F.trim(col_name) == '', True)).alias(f'{col_name}_empty_count')
                ])
            elif isinstance(col_type, BooleanType):
                all_agg_exprs.extend([
                    F.sum(F.when(F.col(col_name) == True, 1).otherwise(0)).alias(f'{col_name}_true_count'),
                    F.sum(F.when(F.col(col_name) == False, 1).otherwise(0)).alias(f'{col_name}_false_count'),
                    F.avg(F.col(col_name).cast("int")).alias(f'{col_name}_true_frequency')
                ])
            
            column_configs[col_name] = col_type
        
        # Execute all aggregations in a single pass
        logger.info(f"Executing {len(all_agg_exprs)} aggregations")
        stats_row = df.select(*all_agg_exprs).first()
        
        # Process results for each column
        logger.info(f"Processing aggregation results for {len(column_configs)} columns")
        for col_name, col_type in column_configs.items():
            try:
                # Build stats dictionary with total_count from earlier calculation
                stats = {
                    'total_count': total_count,
                    'null_count': stats_row[f'{col_name}_null_count'],
                    'distinct_count': stats_row[f'{col_name}_distinct_count']
                }
                
                # Add type-specific stats
                if isinstance(col_type, (IntegerType, LongType, FloatType, DoubleType, DecimalType)):
                    mean = stats_row[f'{col_name}_mean']
                    stddev = stats_row[f'{col_name}_stddev']
                    median = stats_row[f'{col_name}_median']
                    stats.update({
                        'min_value': stats_row[f'{col_name}_min_value'],
                        'max_value': stats_row[f'{col_name}_max_value'],
                        'mean': float(mean) if mean is not None else None,
                        'median': float(median) if median is not None else None,
                        'stddev': float(stddev) if stddev is not None else None
                    })
                    
                    # Calculate extreme values in a separate step if needed
                    if mean is not None and stddev is not None:
                        extreme_count = df.filter(
                            F.abs((F.col(col_name) - mean) / stddev) > 3.0
                        ).count()
                        stats['extreme_count'] = extreme_count
                
                elif isinstance(col_type, DateType):
                    stats.update({
                        'earliest_date': stats_row[f'{col_name}_earliest_date'],
                        'latest_date': stats_row[f'{col_name}_latest_date']
                    })
                
                elif isinstance(col_type, StringType):
                    avg_length = stats_row[f'{col_name}_avg_length']
                    median_length = stats_row[f'{col_name}_median_length']
                    stats.update({
                        'avg_length': float(avg_length) if avg_length is not None else None,
                        'median_length': float(median_length) if median_length is not None else None,
                        'max_length': stats_row[f'{col_name}_max_length'],
                        'empty_count': stats_row[f'{col_name}_empty_count']
                    })
                    
                elif isinstance(col_type, BooleanType):
                    stats.update({
                        'true_count': int(stats_row[f'{col_name}_true_count']) if stats_row[f'{col_name}_true_count'] is not None else 0,
                        'false_count': int(stats_row[f'{col_name}_false_count']) if stats_row[f'{col_name}_false_count'] is not None else 0,
                        'true_frequency': float(stats_row[f'{col_name}_true_frequency']) if stats_row[f'{col_name}_true_frequency'] is not None else None
                    })
                
                # Update historical profiles
                if col_name not in self.historical_profiles["columns"]:
                    self.historical_profiles["columns"][col_name] = []
                self.historical_profiles["columns"][col_name].append(stats)
                
                # Check for suspicious values
                suspicious = {}
                
                # High null ratio check for all types
                null_ratio = stats['null_count'] / total_count if total_count > 0 else 0
                if null_ratio > self.suspicious_thresholds["null_ratio"]:
                    suspicious['high_null_ratio'] = float(null_ratio)
                
                # Type-specific suspicious checks
                if isinstance(col_type, (IntegerType, LongType, FloatType, DoubleType, DecimalType)):
                    if stats.get('extreme_count', 0) > 0:
                        suspicious['extreme_values_count'] = stats['extreme_count']
                elif isinstance(col_type, BooleanType):
                    non_null_count = total_count - stats['null_count']
                    if non_null_count > 0:
                        true_ratio = stats['true_count'] / non_null_count
                        if true_ratio >= 0.75 or true_ratio <= 0.25:
                            suspicious['extreme_imbalance'] = float(true_ratio)
                elif isinstance(col_type, StringType):
                    if total_count - stats['null_count'] > 0:
                        unique_ratio = stats['distinct_count'] / (total_count - stats['null_count'])
                        if unique_ratio > self.suspicious_thresholds["unique_ratio"]:
                            suspicious['high_cardinality'] = float(unique_ratio)
                
                if suspicious:
                    suspicious_values[col_name] = suspicious
                    
            except Exception as e:
                logger.error(f"Error processing stats for {col_name}: {str(e)}")
                continue
        
        # Save updated profiles
        self._save_historical_profiles()
        
        return suspicious_values 