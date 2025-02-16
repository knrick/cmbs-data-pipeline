import pytest
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as F

from spark_processors.schema_manager import SchemaManager

# Schema configurations for different test cases
SCHEMA_CONFIGS = {
    # Basic validation fields
    "basic": {
        "assetNumber": {"type": "string", "nullable": True, "size": 6},
        "reportingPeriodEndDate": {"type": "date", "nullable": True}
    },
    
    # String validation fields
    "strings": {
        "str_required_empty": {"type": "string", "nullable": False, "size": 50},
        "str_required_space": {"type": "string", "nullable": False, "size": 50},
        "str_optional_long": {"type": "string", "nullable": True, "size": 50},
        "str_control_chars": {"type": "string", "nullable": True, "size": 50, "allow_control_chars": False}
    },
    
    # String trimming fields (separate to avoid validation interference)
    "string_trimming": {
        "str_trim_spaces": {"type": "string", "nullable": True, "size": 50, "allow_control_chars": True},
        "str_trim_tabs": {"type": "string", "nullable": True, "size": 50, "allow_control_chars": True},
        "str_trim_mixed": {"type": "string", "nullable": True, "size": 50, "allow_control_chars": True}
    },
    
    # Floating point validation fields
    "floats": {
        "float_pos_valid": {"type": "Float32", "nullable": True},
        "float_neg_valid": {"type": "Float32", "nullable": True},
        "float_pos_beyond": {"type": "Float32", "nullable": True},
        "float_neg_beyond": {"type": "Float32", "nullable": True},
        "double_pos_valid": {"type": "Float64", "nullable": True},
        "double_neg_valid": {"type": "Float64", "nullable": True},
        "double_pos_beyond": {"type": "Float64", "nullable": True},
        "double_neg_beyond": {"type": "Float64", "nullable": True}
    },
    
    # Unsigned integer validation fields
    "unsigned": {
        "uint8_min": {"type": "UInt8", "nullable": True},
        "uint8_max": {"type": "UInt8", "nullable": True},
        "uint8_beyond": {"type": "UInt8", "nullable": True},
        "uint8_negative": {"type": "UInt8", "nullable": True},
        "uint16_min": {"type": "UInt16", "nullable": True},
        "uint16_max": {"type": "UInt16", "nullable": True},
        "uint16_beyond": {"type": "UInt16", "nullable": True},
        "uint16_negative": {"type": "UInt16", "nullable": True}
    },
    
    # Boolean transformation fields
    "booleans": {
        "bool_true_str": {"type": "bool", "nullable": False},
        "bool_true_upper": {"type": "bool", "nullable": False},
        "bool_true_one": {"type": "bool", "nullable": False},
        "bool_true_yes": {"type": "bool", "nullable": False},
        "bool_true_t": {"type": "bool", "nullable": False},
        "bool_true_y": {"type": "bool", "nullable": False},
        "bool_false_str": {"type": "bool", "nullable": False},
        "bool_false_upper": {"type": "bool", "nullable": False},
        "bool_false_zero": {"type": "bool", "nullable": False},
        "bool_false_no": {"type": "bool", "nullable": False},
        "bool_false_f": {"type": "bool", "nullable": False},
        "bool_false_n": {"type": "bool", "nullable": False}
    },
    
    # Decimal precision fields
    "decimals": {
        "decimal_3_2": {"type": "number", "whole": 3, "decimal": 2, "nullable": False},
        "decimal_5_2": {"type": "number", "whole": 5, "decimal": 2, "nullable": False},
        "decimal_7_2": {"type": "number", "whole": 7, "decimal": 2, "nullable": False},
        "decimal_9_2": {"type": "number", "whole": 9, "decimal": 2, "nullable": False},
        "decimal_11_2": {"type": "number", "whole": 11, "decimal": 2, "nullable": False},
        "decimal_13_2": {"type": "number", "whole": 13, "decimal": 2, "nullable": False}
    },
    
    # Date validation fields
    "dates": {
        "date_required_mmddyyyy": {"type": "date", "nullable": False},
        "date_required_yyyymmdd": {"type": "date", "nullable": False},
        "date_optional_mmddyyyy": {"type": "date", "nullable": True},
        "date_invalid_format": {"type": "date", "nullable": False},
        "date_invalid_value": {"type": "date", "nullable": False}
    }
}

@pytest.fixture(scope="module")
def spark():
    """Create a Spark session for testing."""
    return SparkSession.builder \
        .appName("test_schema_manager") \
        .master("local[*]") \
        .getOrCreate()

@pytest.fixture
def schema_manager():
    """Create a SchemaManager instance."""
    return SchemaManager()

def test_schema_validation(schema_manager, spark):
    """Test DataFrame schema validation for basic data types."""
    # Update schema with only the fields we need for this test
    schema_manager.loan_schema = {
        "version": 1,
        "type": "loan",
        "columns": SCHEMA_CONFIGS["basic"]
    }
    
    # Create test DataFrame with mixed types
    data = [
        # Test valid values for each type
        ("A12345", "2024-01-01", "loan"),
        # Test edge cases for each type
        ("B67890", "2023-12-31", "loan"),
        ("C11111", "2025-06-15", "loan"),
        # Test different formats
        ("D22222", "01-01-2024", "loan"),
        # Test nulls (all nullable fields)
        (None, None, "loan")
    ]
    df = spark.createDataFrame(data, [
        "assetNumber",
        "reportingPeriodEndDate",
        "_validation_type"
    ])
    
    # Validate DataFrame against predefined schema
    validated_df = schema_manager.validate_dataframe_schema(df, "loan")
    
    # Check types were correctly cast
    assert isinstance(validated_df.schema["assetNumber"].dataType, StringType)
    assert isinstance(validated_df.schema["reportingPeriodEndDate"].dataType, DateType)
    
    # Verify values were correctly transformed
    first_row = validated_df.first()
    assert first_row.assetNumber == "A12345"
    assert str(first_row.reportingPeriodEndDate) == "2024-01-01"


def test_date_validation(schema_manager, spark):
    """Test validation of various date formats and edge cases."""
    # Update schema with date validation fields
    schema_manager.loan_schema = {
        "version": 1,
        "type": "loan",
        "columns": SCHEMA_CONFIGS["dates"]
    }
    
    # First test invalid cases
    data = [
        # Mix of valid and invalid formats
        (
            "01-01-2024",
            "2024-01-01",
            "12-31-2023",
            "2024/01/01",
            "13-32-2024",
            "loan"
        ),
        # Test null handling
        (
            None,
            None,
            None,
            None,
            None,
            "loan"
        )
    ]
    df = spark.createDataFrame(data, [
        "date_required_mmddyyyy",
        "date_required_yyyymmdd",
        "date_optional_mmddyyyy",
        "date_invalid_format",
        "date_invalid_value",
        "_validation_type"
    ])
    
    # Should raise error for invalid dates
    with pytest.raises(ValueError) as exc_info:
        schema_manager.validate_dataframe_schema(df, "loan")
    
    error_msg = str(exc_info.value)
    
    # Check format validations
    assert "invalid date values in date_invalid_format" in error_msg
    assert "invalid date values in date_invalid_value" in error_msg
    
    # Check null validations
    assert "null values in non-nullable field date_required_mmddyyyy" in error_msg
    assert "null values in non-nullable field date_required_yyyymmdd" in error_msg
    assert "null values in non-nullable field date_invalid_format" in error_msg
    assert "null values in non-nullable field date_invalid_value" in error_msg
    
    # Now test valid transformations with a separate schema
    valid_schema = {
        "version": 1,
        "type": "loan",
        "columns": {
            "date_required_mmddyyyy": SCHEMA_CONFIGS["dates"]["date_required_mmddyyyy"],
            "date_required_yyyymmdd": SCHEMA_CONFIGS["dates"]["date_required_yyyymmdd"],
            "date_optional_mmddyyyy": SCHEMA_CONFIGS["dates"]["date_optional_mmddyyyy"]
        }
    }
    schema_manager.loan_schema = valid_schema
    
    valid_df = spark.createDataFrame([
        ("01-01-2024", "2024-01-01", "12-31-2023", "loan")
    ], [
        "date_required_mmddyyyy",
        "date_required_yyyymmdd",
        "date_optional_mmddyyyy",
        "_validation_type"
    ])
    
    validated_df = schema_manager.validate_dataframe_schema(valid_df, "loan")
    first_row = validated_df.first()
    
    # Both formats should be transformed to yyyy-MM-dd
    assert str(first_row.date_required_mmddyyyy) == "2024-01-01"
    assert str(first_row.date_required_yyyymmdd) == "2024-01-01"
    assert str(first_row.date_optional_mmddyyyy) == "2023-12-31"

def test_unsigned_integer_validation(schema_manager, spark):
    """Test validation of unsigned integer types with various edge cases."""
    # Update schema with unsigned integer fields
    schema_manager.loan_schema = {
        "version": 1,
        "type": "loan",
        "columns": SCHEMA_CONFIGS["unsigned"]
    }
    
    data = [
        (
            0,
            255,
            256,
            -1,
            0,
            65535,
            65536,
            -1,
            "loan"
        )
    ]
    df = spark.createDataFrame(data, [
        "uint8_min",
        "uint8_max",
        "uint8_beyond",
        "uint8_negative",
        "uint16_min",
        "uint16_max",
        "uint16_beyond",
        "uint16_negative",
        "_validation_type"
    ])
    
    # Should raise error for invalid unsigned values
    with pytest.raises(ValueError) as exc_info:
        schema_manager.validate_dataframe_schema(df, "loan")
    
    error_msg = str(exc_info.value)
    
    # Valid UInt8 values should not trigger errors
    assert "outside UInt8 range in uint8_min" not in error_msg
    assert "outside UInt8 range in uint8_max" not in error_msg
    
    # Invalid UInt8 values should trigger errors
    assert "outside UInt8 range in uint8_beyond" in error_msg
    assert "outside UInt8 range in uint8_negative" in error_msg
    
    # Valid UInt16 values should not trigger errors
    assert "outside UInt16 range in uint16_min" not in error_msg
    assert "outside UInt16 range in uint16_max" not in error_msg
    
    # Invalid UInt16 values should trigger errors
    assert "outside UInt16 range in uint16_beyond" in error_msg
    assert "outside UInt16 range in uint16_negative" in error_msg

def test_value_transformations(schema_manager, spark):
    """Test value transformations for different data types."""
    # Update schema with transformation test fields
    schema_manager.loan_schema = {
        "version": 1,
        "type": "loan",
        "columns": SCHEMA_CONFIGS["booleans"]
    }
    
    data = [
        # Test various input formats
        (
            "true", "TRUE", "1", "yes", "t", "y",
            "false", "FALSE", "0", "no", "f", "n",
            "loan"
        )
    ]
    df = spark.createDataFrame(data, [
        "bool_true_str", "bool_true_upper", "bool_true_one", "bool_true_yes", "bool_true_t", "bool_true_y",
        "bool_false_str", "bool_false_upper", "bool_false_zero", "bool_false_no", "bool_false_f", "bool_false_n",
        "_validation_type"
    ])
    
    validated_df = schema_manager.validate_dataframe_schema(df, "loan")
    
    # Verify transformations
    first_row = validated_df.first()
    assert first_row.bool_true_str == True
    assert first_row.bool_true_upper == True
    assert first_row.bool_true_one == True
    assert first_row.bool_true_yes == True
    assert first_row.bool_true_t == True
    assert first_row.bool_true_y == True
    assert first_row.bool_false_str == False
    assert first_row.bool_false_upper == False
    assert first_row.bool_false_zero == False
    assert first_row.bool_false_no == False
    assert first_row.bool_false_f == False
    assert first_row.bool_false_n == False

def test_decimal_precision_validation(schema_manager, spark):
    """Test validation of decimal precision with various cases."""
    # Update schema with decimal precision fields
    schema_manager.loan_schema = {
        "version": 1,
        "type": "loan",
        "columns": SCHEMA_CONFIGS["decimals"]
    }
    
    data = [
        # Test various decimal precisions
        ("123.45", "12345.67", "1234567.89", "123456789.12", "12345678901.12", "1234567890123.12", "loan"),
        ("1234.56", "123456.78", "12345678.90", "1234567890.12", "123456789012.12", "12345678901234.12", "loan"),
        ("123.456", "12345.678", "1234567.890", "123456789.123", "12345678901.123", "1234567890123.123", "loan"),
        ("abc", "-", "1.2.3", "not_a_number", "invalid", "error", "loan")
    ]
    df = spark.createDataFrame(data, [
        "decimal_3_2",
        "decimal_5_2",
        "decimal_7_2",
        "decimal_9_2",
        "decimal_11_2",
        "decimal_13_2",
        "_validation_type"
    ])
    
    # Should raise error for precision violations
    with pytest.raises(ValueError) as exc_info:
        schema_manager.validate_dataframe_schema(df, "loan")
    
    error_msg = str(exc_info.value)
    assert "numeric value exceeds 3.2 format in decimal_3_2" in error_msg
    assert "numeric value exceeds 5.2 format in decimal_5_2" in error_msg
    assert "numeric value exceeds 7.2 format in decimal_7_2" in error_msg
    assert "numeric value exceeds 9.2 format in decimal_9_2" in error_msg
    assert "numeric value exceeds 11.2 format in decimal_11_2" in error_msg
    assert "numeric value exceeds 13.2 format in decimal_13_2" in error_msg

def test_floating_point_validation(schema_manager, spark):
    """Test validation of floating point numbers with various precisions."""
    # Update schema with floating point fields
    schema_manager.loan_schema = {
        "version": 1,
        "type": "loan",
        "columns": SCHEMA_CONFIGS["floats"]
    }
    
    data = [
        (
            1.23e38,
            -1.23e38,
            3.5e38,
            -3.5e38,
            1.23e100,
            -1.23e100,
            1.9e308,
            -1.9e308,
            "loan"
        )
    ]
    df = spark.createDataFrame(data, [
        "float_pos_valid",
        "float_neg_valid",
        "float_pos_beyond",
        "float_neg_beyond",
        "double_pos_valid",
        "double_neg_valid",
        "double_pos_beyond",
        "double_neg_beyond",
        "_validation_type"
    ])
    
    # Should raise error for values beyond precision
    with pytest.raises(ValueError) as exc_info:
        schema_manager.validate_dataframe_schema(df, "loan")
    
    error_msg = str(exc_info.value)
    
    # Valid Float32 values should not trigger errors
    assert "invalid floating-point values in float_pos_valid" not in error_msg
    assert "invalid floating-point values in float_neg_valid" not in error_msg
    
    # Values beyond Float32 range should trigger errors
    assert "invalid floating-point values in float_pos_beyond" in error_msg
    assert "invalid floating-point values in float_neg_beyond" in error_msg
    
    # Values valid for Float64 but beyond Float32 should not trigger errors in Float64 columns
    assert "invalid floating-point values in double_pos_valid" not in error_msg
    assert "invalid floating-point values in double_neg_valid" not in error_msg
    
    # Values beyond Float64 range should trigger errors
    assert "invalid floating-point values in double_pos_beyond" in error_msg
    assert "invalid floating-point values in double_neg_beyond" in error_msg

def test_string_trimming(schema_manager, spark):
    """Test string trimming behavior separately from validation."""
    # Update schema with only trimming test fields
    schema_manager.loan_schema = {
        "version": 1,
        "type": "loan",
        "columns": SCHEMA_CONFIGS["string_trimming"]
    }
    
    # Create test data with various whitespace patterns
    data = [
        (
            "  valid  ",
            "\tvalid\n",
            " \t valid \n ",
            "loan"
        )
    ]
    df = spark.createDataFrame(data, [
        "str_trim_spaces",
        "str_trim_tabs",
        "str_trim_mixed",
        "_validation_type"
    ])
    
    # Validate and check trimming
    validated_df = schema_manager.validate_dataframe_schema(df, "loan")
    first_row = validated_df.first()
    
    # Verify trimming behavior
    assert first_row.str_trim_spaces == "valid"
    assert first_row.str_trim_tabs == "valid"
    assert first_row.str_trim_mixed == "valid"

def test_string_validation(schema_manager, spark):
    """Test validation of string fields with various edge cases."""
    # Update schema with string validation fields
    schema_manager.loan_schema = {
        "version": 1,
        "type": "loan",
        "columns": SCHEMA_CONFIGS["strings"]
    }
    
    # Create test data
    long_string = "x" * 51 
    
    data = [
        (
            "",
            " ",
            long_string,
            "control\tchar",
            "loan"
        )
    ]
    df = spark.createDataFrame(data, [
        "str_required_empty",
        "str_required_space",
        "str_optional_long",
        "str_control_chars",
        "_validation_type"
    ])
    
    # Test validation errors
    with pytest.raises(ValueError) as exc_info:
        schema_manager.validate_dataframe_schema(df, "loan")
    
    error_msg = str(exc_info.value)
    
    # Empty strings in required fields should trigger errors
    assert "empty string values in str_required_empty" in error_msg
    assert "empty string values in str_required_space" in error_msg  # After trimming " " becomes ""
    
    # String length exceeding limit should trigger error
    assert "string length exceeds 50 in str_optional_long" in error_msg

    # Control characters should trigger error
    assert "control characters in str_control_chars" in error_msg

def test_unknown_columns(schema_manager, spark):
    """Test handling of unknown and missing columns."""
    # Update schema with minimal required fields
    schema_manager.loan_schema = {
        "version": 1,
        "type": "loan",
        "columns": {
            "assetNumber": SCHEMA_CONFIGS["basic"]["assetNumber"],
            "reportingPeriodEndDate": SCHEMA_CONFIGS["basic"]["reportingPeriodEndDate"]
        }
    }
    
    # Test cases for unknown and missing columns
    data = [
        # Include both valid and invalid columns
        ("A12345", "2024-01-01", "extra1", 123, "loan"),
        ("B67890", "2023-12-31", "extra2", 456, "loan")
    ]
    df = spark.createDataFrame(data, [
        "assetNumber",
        "reportingPeriodEndDate",
        "unknown_string",
        "unknown_number",
        "_validation_type"
    ])
    
    # Should raise error for unknown columns
    with pytest.raises(ValueError) as exc_info:
        schema_manager.validate_dataframe_schema(df, "loan")
    
    error_msg = str(exc_info.value)
    assert "Found columns in DataFrame that are not defined in schema" in error_msg
    assert "unknown_string" in error_msg
    assert "unknown_number" in error_msg 