import pytest
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as F

from spark_processors.schema_manager import SchemaManager

@pytest.fixture(scope="module")
def spark():
    """Create a Spark session for testing."""
    return SparkSession.builder \
        .appName("test_schema_manager") \
        .master("local[*]") \
        .getOrCreate()

@pytest.fixture(scope="module")
def schema_manager():
    """Create a SchemaManager instance with test schemas."""
    manager = SchemaManager()
    
    # Set up loan schema for tests
    manager.loan_schema = {
        "version": 1,
        "columns": {
            # Schema validation test fields
            "assetNumber": {"type": "string", "nullable": True},
            "reportingPeriodEndDate": {"type": "date", "nullable": True},
            "intValue": {"type": "Int32", "nullable": True},
            "boolValue": {"type": "bool", "nullable": True},
            "decimalValue": {"type": "Decimal(10,2)", "nullable": True},
            "tinyIntValue": {"type": "Int8", "nullable": True},
            "smallIntValue": {"type": "Int16", "nullable": True},
            "timestampValue": {"type": "datetime", "nullable": True},
            
            # Invalid values test fields
            "stringValue": {"type": "string", "nullable": False},
            "dateValue": {"type": "date", "nullable": False},
            
            # Date validation test fields
            "date1": {"type": "date", "nullable": True},
            "date2": {"type": "date", "nullable": True},
            "date3": {"type": "date", "nullable": True},
            "date4": {"type": "date", "nullable": True},
            "date5": {"type": "date", "nullable": True},
            
            # Unsigned integer test fields
            "uint8_1": {"type": "UInt8", "nullable": False},
            "uint8_2": {"type": "UInt8", "nullable": False},
            "uint16_1": {"type": "UInt16", "nullable": False},
            "uint16_2": {"type": "UInt16", "nullable": False},
            
            # Boolean transformation test fields
            "bool1": {"type": "bool", "nullable": False},
            "bool2": {"type": "bool", "nullable": False},
            "bool3": {"type": "bool", "nullable": False},
            "bool4": {"type": "bool", "nullable": False},
            "bool5": {"type": "bool", "nullable": False},
            "bool6": {"type": "bool", "nullable": False},
            
            # Numeric transformation test fields
            "num1": {"type": "Int32", "nullable": False},
            "num2": {"type": "Float64", "nullable": False},
            "num3": {"type": "Float64", "nullable": False},
            "num4": {"type": "Int32", "nullable": False},
            "num5": {"type": "Float64", "nullable": False},
            "num6": {"type": "Float64", "nullable": False},
            
            # Floating point test fields
            "float32_1": {"type": "Float32", "nullable": False},
            "float32_2": {"type": "Float32", "nullable": False},
            "float32_3": {"type": "Float32", "nullable": False},
            "float64_1": {"type": "Float64", "nullable": False},
            "float64_2": {"type": "Float64", "nullable": False},
            "float64_3": {"type": "Float64", "nullable": False},
            
            # Empty string test fields
            "required1": {"type": "string", "nullable": False},
            "required2": {"type": "string", "nullable": False},
            "required3": {"type": "string", "nullable": False},
            "optional1": {"type": "string", "nullable": True},
            "optional2": {"type": "string", "nullable": True},
            "optional3": {"type": "string", "nullable": True}
        }
    }
    
    return manager

def test_schema_validation(schema_manager, spark):
    """Test DataFrame schema validation for all data types."""
    # Create test DataFrame with mixed types
    data = [
        # Test all types with valid values
        ("asset1", "01-01-2024", 100, "true", "123.45", 127, 32767, "2024-01-01 12:34:56", "loan"),
        # Test all types with edge cases
        ("asset2", "12-31-2023", -100, "1", "-123.45", -128, -32768, "2023-12-31 23:59:59", "loan"),
        # Test nulls
        (None, None, None, None, None, None, None, None, "loan")
    ]
    df = spark.createDataFrame(data, [
        "assetNumber",
        "reportingPeriodEndDate",
        "intValue",
        "boolValue",
        "decimalValue",
        "tinyIntValue",
        "smallIntValue",
        "timestampValue",
        "_validation_type"
    ])
    
    # Validate DataFrame against predefined schema
    validated_df = schema_manager.validate_dataframe_schema(df, "loan")
    
    # Check types were correctly cast
    assert isinstance(validated_df.schema["assetNumber"].dataType, StringType)
    assert isinstance(validated_df.schema["reportingPeriodEndDate"].dataType, DateType)
    assert isinstance(validated_df.schema["intValue"].dataType, IntegerType)
    assert isinstance(validated_df.schema["boolValue"].dataType, BooleanType)
    assert isinstance(validated_df.schema["decimalValue"].dataType, DecimalType)
    assert isinstance(validated_df.schema["tinyIntValue"].dataType, ByteType)
    assert isinstance(validated_df.schema["smallIntValue"].dataType, ShortType)
    assert isinstance(validated_df.schema["timestampValue"].dataType, TimestampType)

def test_invalid_values(schema_manager, spark):
    """Test parallel validation of invalid values for each data type."""
    # Test multiple invalid values in parallel
    data = [
        # Control chars, invalid numbers, invalid dates, all in one row
        ("asset1\x00", "invalid_bool", 2147483648, "13-32-2024", "123.456", "loan"),
        # Mix of valid and invalid values
        ("asset2", "true", -1, "01-01-2024", "123.45", "loan"),
        # All valid values
        ("asset3", "false", 100, "2024-01-01", "123.4", "loan"),
    ]
    df = spark.createDataFrame(data, [
        "stringValue",
        "boolValue",
        "intValue",
        "dateValue",
        "decimalValue",
        "_validation_type"
    ])
    
    # Should raise error containing all validation errors
    with pytest.raises(ValueError) as exc_info:
        schema_manager.validate_dataframe_schema(df, "loan")
    
    error_msg = str(exc_info.value)
    assert "invalid string values in stringValue" in error_msg
    assert "invalid date values in dateValue" in error_msg
    assert "invalid decimal value in decimalValue" in error_msg

def test_date_validation(schema_manager, spark):
    """Test parallel validation of date formats."""
    data = [
        # Mix of valid and invalid formats
        ("01-01-2024", "2024-01-01", "2024/01/01", "13-32-2024", "01-01-24", "loan"),
        ("12-31-2023", "2023-12-31", "31/12/2023", "00-00-2024", "23-12-31", "loan"),
        (None, None, None, None, None, "loan")  # Test null handling
    ]
    df = spark.createDataFrame(data, [
        "date1",  # MM-dd-yyyy format
        "date2",  # yyyy-MM-dd format
        "date3",  # Invalid format
        "date4",  # Invalid date
        "date5",   # Invalid short format
        "_validation_type"
    ])
    
    # Should raise error for invalid date format
    with pytest.raises(ValueError, match="invalid date values"):
        schema_manager.validate_dataframe_schema(df, "loan")

def test_unsigned_integer_validation(schema_manager, spark):
    """Test parallel validation of unsigned integer types."""
    data = [
        # Mix of valid and invalid values for both UInt8 and UInt16
        (255, 65535, 0, 0, "loan"),      # All valid
        (256, 65536, -1, -1, "loan"),    # All invalid
        (128, 32768, 254, 65534, "loan") # Mixed valid/invalid
    ]
    df = spark.createDataFrame(data, [
        "uint8_1", "uint16_1",  # Test max values
        "uint8_2", "uint16_2",   # Test min values
        "_validation_type"
    ])
    
    # Should raise error for invalid unsigned values
    with pytest.raises(ValueError, match="outside UInt8 range"):
        schema_manager.validate_dataframe_schema(df, "loan")

def test_value_transformations(schema_manager, spark):
    """Test parallel value transformations during validation."""
    # Test boolean transformations
    bool_data = [
        ("TRUE", "true", "1", "yes", "t", "y", "loan"),           # All should become True
        ("FALSE", "false", "0", "no", "f", "n", "loan"),          # All should become False
    ]
    bool_df = spark.createDataFrame(bool_data, ["bool1", "bool2", "bool3", "bool4", "bool5", "bool6", "_validation_type"])
    
    validated_bool_df = schema_manager.validate_dataframe_schema(bool_df, "loan")
    bool_row = validated_bool_df.select("bool1", "bool2", "bool3", "bool4", "bool5", "bool6").first()
    assert all(bool_row)  # First row should all be True
    
    # Test numeric transformations
    num_data = [
        ("123", "-123.45", "123.00", "+123", "1.23e2", "-1.23e2", "loan"),  # Various numeric formats
        ("456", "456.78", "456.00", "-456", "4.56e2", "4.56e2", "loan")     # More numeric values
    ]
    num_df = spark.createDataFrame(num_data, ["num1", "num2", "num3", "num4", "num5", "num6", "_validation_type"])
    
    validated_num_df = schema_manager.validate_dataframe_schema(num_df, "loan")
    num_row = validated_num_df.select("num1", "num2", "num3", "num4", "num5", "num6").first()
    assert num_row.num1 == 123
    assert num_row.num2 == -123.45
    assert abs(num_row.num5 - 123.0) < 0.001

def test_floating_point_validation(schema_manager, spark):
    """Test parallel validation of floating point numbers."""
    data = [
        # Test multiple floating point validations in parallel
        ("1.23e-4", "1.23E+4", "1.23e4", "1.23e-4", "1.23E+4", "1.23e4", "loan"),
        ("1.8e+308", "-1.8e+308", "2.23e-308", "1.8e+308", "-1.8e+308", "2.23e-308", "loan"),  # Beyond double precision
        ("123.456789012345", "123.45678901234567890", "0.1234567890123456", 
         "123.456789012345", "123.45678901234567890", "0.1234567890123456", "loan")
    ]
    df = spark.createDataFrame(data, [
        "float32_1", "float32_2", "float32_3",  # Test Float32
        "float64_1", "float64_2", "float64_3",   # Test Float64
        "_validation_type"
    ])
    
    # Should raise error for values beyond precision
    with pytest.raises(ValueError, match="invalid floating-point values"):
        schema_manager.validate_dataframe_schema(df, "loan")

def test_empty_string_validation(schema_manager, spark):
    """Test parallel validation of empty and whitespace strings."""
    data = [
        # Test multiple string validations in parallel
        ("", None, " ", "\t", "\n", "\r", "loan"),          # Empty and whitespace
        ("value1", "value2", " value3 ", "\tval4", "val5\n", "val6\r", "loan"),  # Valid with whitespace
        (" ", "  ", "\t\t", "\n\n", "\r\r", "   ", "loan")  # All whitespace
    ]
    df = spark.createDataFrame(data, [
        "required1", "optional1",  # Empty string vs null
        "required2", "optional2",  # Whitespace strings
        "required3", "optional3",   # Line endings
        "_validation_type"
    ])
    
    # Should raise error for empty strings in non-nullable fields
    with pytest.raises(ValueError, match="empty string values"):
        schema_manager.validate_dataframe_schema(df, "loan")

def test_unknown_columns(schema_manager, spark):
    """Test that an error is raised when DataFrame has columns not in schema."""
    # Create test DataFrame with an unknown column
    data = [
        ("asset1", "01-01-2024", "unknown_value", "loan"),
        ("asset2", "12-31-2023", "another_value", "loan")
    ]
    df = spark.createDataFrame(data, [
        "assetNumber",           # Known column
        "reportingPeriodEndDate", # Known column
        "unknown_column",         # Column not in schema
        "_validation_type"
    ])
    
    # Should raise error for unknown column
    with pytest.raises(ValueError, match="Found columns in DataFrame that are not defined in schema"):
        schema_manager.validate_dataframe_schema(df, "loan") 