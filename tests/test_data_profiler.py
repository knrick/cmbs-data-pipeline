import pytest
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as F

from spark_processors.data_profiler import DataProfiler

@pytest.fixture(scope="module")
def spark():
    """Create a Spark session for testing."""
    return SparkSession.builder \
        .appName("test_data_profiler") \
        .master("local[*]") \
        .getOrCreate()

@pytest.fixture(scope="module")
def data_profiler():
    """Create a DataProfiler instance."""
    return DataProfiler()

def test_check_duplicates(spark, data_profiler):
    """Test duplicate detection."""
    # Create test data with duplicates
    data = [
        ("A1", datetime(2023, 1, 1), "file1.xml"),
        ("A1", datetime(2023, 1, 1), "file2.xml"),  # Duplicate
        ("A2", datetime(2023, 1, 2), "file1.xml")
    ]
    df = spark.createDataFrame(
        data,
        ["assetNumber", "reportingPeriodEndDate", "source_file"]
    )
    
    # Check duplicates
    result = data_profiler.check_duplicates(df)
    
    assert result is not None
    assert result["duplicate_count"] == 1
    assert len(result["duplicate_info"]) == 1
    assert result["duplicate_info"][0]["assetNumber"] == "A1"
    assert result["duplicate_info"][0]["count"] == 2
    assert len(result["duplicate_info"][0]["source_files"]) == 2

def test_get_column_stats_numeric(spark, data_profiler):
    """Test statistics calculation for numeric columns."""
    data = [
        (1,), (2,), (3,), (None,), (5,)
    ]
    df = spark.createDataFrame(data, ["numeric_col"])
    
    stats = data_profiler.get_column_stats(df, "numeric_col")
    
    assert stats["total_count"] == 5
    assert stats["null_count"] == 1
    assert stats["distinct_count"] == 5
    assert stats["min_value"] == 1
    assert stats["max_value"] == 5
    assert "mean" in stats
    assert "stddev" in stats

def test_get_column_stats_boolean(spark, data_profiler):
    """Test statistics calculation for boolean columns."""
    data = [
        (True,), (False,), (True,), (None,), (True,)
    ]
    df = spark.createDataFrame(data, ["bool_col"])
    
    stats = data_profiler.get_column_stats(df, "bool_col")
    
    assert stats["total_count"] == 5
    assert stats["null_count"] == 1
    assert stats["distinct_count"] == 3
    assert stats["true_count"] == 3
    assert stats["false_count"] == 1
    assert "mean" not in stats
    assert "stddev" not in stats

def test_get_column_stats_string(spark, data_profiler):
    """Test statistics calculation for string columns."""
    data = [
        ("A",), ("B",), ("A",), (None,), ("C",)
    ]
    df = spark.createDataFrame(data, ["string_col"])
    
    stats = data_profiler.get_column_stats(df, "string_col")
    
    assert stats["total_count"] == 5
    assert stats["null_count"] == 1
    assert stats["distinct_count"] == 4
    assert stats["min_value"] == "A"
    assert stats["max_value"] == "C"
    assert "mean" not in stats
    assert "stddev" not in stats

def test_check_boolean_suspicious(spark, data_profiler):
    """Test suspicious value detection for boolean columns."""
    # Test high null ratio
    data1 = [(True,), (None,), (None,), (None,)]
    df1 = spark.createDataFrame(data1, ["bool_col"])
    suspicious_values = data_profiler.update_profile(df1)
    
    assert "bool_col" in suspicious_values
    assert "high_null_ratio" in suspicious_values["bool_col"]
    assert suspicious_values["bool_col"]["high_null_ratio"] > 0.5
    
    # Test extreme imbalance
    data2 = [(True,), (True,), (True,), (False,)]
    df2 = spark.createDataFrame(data2, ["bool_col"])
    suspicious_values = data_profiler.update_profile(df2)
    
    assert "bool_col" in suspicious_values
    assert "extreme_imbalance" in suspicious_values["bool_col"]
    assert suspicious_values["bool_col"]["extreme_imbalance"] >= 0.75

def test_update_profile(spark, data_profiler):
    """Test profile updating with different column types."""
    # Create test data with multiple column types
    data = [
        (1, True, "A", "2023-01-01"),
        (2, False, "B", "2023-01-02"),
        (None, None, None, None),
        (4, True, "D", "2023-01-04")
    ]
    df = spark.createDataFrame(
        data,
        ["numeric_col", "bool_col", "string_col", "date_col"]
    )
    
    suspicious_values = data_profiler.update_profile(df)  # schema_manager not needed for test
    
    # Check that profiles were created for each column
    assert "numeric_col" in data_profiler.historical_profiles["columns"]
    assert "bool_col" in data_profiler.historical_profiles["columns"]
    assert "string_col" in data_profiler.historical_profiles["columns"]
    assert "date_col" in data_profiler.historical_profiles["columns"]
    
    # Check that suspicious values were detected appropriately
    if "bool_col" in suspicious_values:
        assert suspicious_values["bool_col"] is not None
        assert any(key in suspicious_values["bool_col"] 
                  for key in ["high_null_ratio", "extreme_imbalance"])