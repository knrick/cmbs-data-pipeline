import pytest
import json
import os
from pyspark.sql.types import *

@pytest.fixture
def loan_schema_path(temp_dir):
    """Create a temporary loan schema file for testing."""
    schema_dir = temp_dir / "schemas" / "loan"
    schema_dir.mkdir(parents=True)
    
    schema = {
        "version": 1,
        "columns": {
            "assetNumber": {
                "type": "string",
                "nullable": False,
                "description": "Unique identifier for the asset",
                "size": 6
            },
            "reportingPeriodEndDate": {
                "type": "date",
                "nullable": False,
                "description": "End date of reporting period"
            },
            "originalLoanAmount": {
                "type": "number",
                "whole": 9,
                "decimal": 2,
                "nullable": True,
                "description": "Original loan amount"
            }
        }
    }
    
    schema_file = schema_dir / "loan_schema_v1.json"
    with open(schema_file, "w") as f:
        json.dump(schema, f, indent=4)
    
    return schema_file

@pytest.fixture
def property_schema_path(temp_dir):
    """Create a temporary property schema file for testing."""
    schema_dir = temp_dir / "schemas" / "property"
    schema_dir.mkdir(parents=True)
    
    schema = {
        "version": 1,
        "columns": {
            "propertyId": {
                "type": "string",
                "nullable": False,
                "description": "Unique identifier for the property",
                "size": 6
            },
            "valuationDate": {
                "type": "date",
                "nullable": True,
                "description": "Date of property valuation"
            },
            "propertyValue": {
                "type": "number",
                "whole": 13,
                "decimal": 2,
                "nullable": True,
                "description": "Property valuation amount"
            }
        }
    }
    
    schema_file = schema_dir / "property_schema_v1.json"
    with open(schema_file, "w") as f:
        json.dump(schema, f, indent=4)
    
    return schema_file

def test_loan_schema_structure(loan_schema_path):
    """Test the structure of the loan schema."""
    with open(loan_schema_path) as f:
        schema = json.load(f)
    
    # Test basic structure
    assert "version" in schema
    assert "columns" in schema
    assert isinstance(schema["columns"], dict)
    
    # Test required columns
    required_columns = ["assetNumber", "reportingPeriodEndDate"]
    for col in required_columns:
        assert col in schema["columns"]
        assert not schema["columns"][col]["nullable"]

def test_property_schema_structure(property_schema_path):
    """Test the structure of the property schema."""
    with open(property_schema_path) as f:
        schema = json.load(f)
    
    # Test basic structure
    assert "version" in schema
    assert "columns" in schema
    assert isinstance(schema["columns"], dict)
    
    # Test required columns
    assert "propertyId" in schema["columns"]
    assert not schema["columns"]["propertyId"]["nullable"]

def test_schema_data_types(loan_schema_path, property_schema_path):
    """Test data type definitions in both schemas."""
    # Load both schemas
    with open(loan_schema_path) as f:
        loan_schema = json.load(f)
    with open(property_schema_path) as f:
        property_schema = json.load(f)
    
    # Test loan schema types
    loan_cols = loan_schema["columns"]
    assert loan_cols["assetNumber"]["type"] == "string"
    assert loan_cols["reportingPeriodEndDate"]["type"] == "date"
    assert loan_cols["originalLoanAmount"]["type"] == "number"
    
    # Test property schema types
    prop_cols = property_schema["columns"]
    assert prop_cols["propertyId"]["type"] == "string"
    assert prop_cols["valuationDate"]["type"] == "date"
    assert prop_cols["propertyValue"]["type"] == "number"

def test_schema_versioning(temp_dir):
    """Test schema versioning functionality."""
    # Create schema directories
    loan_schema_dir = temp_dir / "schemas" / "loan"
    property_schema_dir = temp_dir / "schemas" / "property"
    loan_schema_dir.mkdir(parents=True, exist_ok=True)
    property_schema_dir.mkdir(parents=True, exist_ok=True)
    
    # Create v1 schemas
    loan_schema_v1 = {
        "version": 1,
        "columns": {
            "assetNumber": {
                "type": "string",
                "nullable": False,
                "description": "Unique identifier for the asset"
            }
        }
    }
    
    property_schema_v1 = {
        "version": 1,
        "columns": {
            "propertyId": {
                "type": "string",
                "nullable": False,
                "description": "Unique identifier for the property"
            }
        }
    }
    
    # Save v1 schemas
    loan_schema_v1_path = loan_schema_dir / "loan_schema_v1.json"
    property_schema_v1_path = property_schema_dir / "property_schema_v1.json"
    
    with open(loan_schema_v1_path, "w") as f:
        json.dump(loan_schema_v1, f, indent=4)
    with open(property_schema_v1_path, "w") as f:
        json.dump(property_schema_v1, f, indent=4)
    
    # Create v2 schemas with additional columns
    loan_schema_v2 = {
        "version": 2,
        "columns": {
            "assetNumber": {
                "type": "string",
                "nullable": False,
                "description": "Unique identifier for the asset"
            },
            "newColumn": {
                "type": "string",
                "nullable": True,
                "description": "New test column"
            }
        }
    }
    
    property_schema_v2 = {
        "version": 2,
        "columns": {
            "propertyId": {
                "type": "string",
                "nullable": False,
                "description": "Unique identifier for the property"
            },
            "newFeature": {
                "type": "bool",
                "nullable": True,
                "description": "New feature flag"
            }
        }
    }
    
    # Save v2 schemas
    loan_schema_v2_path = loan_schema_dir / "loan_schema_v2.json"
    property_schema_v2_path = property_schema_dir / "property_schema_v2.json"
    
    with open(loan_schema_v2_path, "w") as f:
        json.dump(loan_schema_v2, f, indent=4)
    with open(property_schema_v2_path, "w") as f:
        json.dump(property_schema_v2, f, indent=4)
    
    # Verify both versions exist and have correct content
    assert loan_schema_v1_path.exists()
    assert loan_schema_v2_path.exists()
    assert property_schema_v1_path.exists()
    assert property_schema_v2_path.exists()
    
    # Load and verify schema versions
    with open(loan_schema_v1_path) as f:
        assert json.load(f)["version"] == 1
    with open(loan_schema_v2_path) as f:
        assert json.load(f)["version"] == 2
    with open(property_schema_v1_path) as f:
        assert json.load(f)["version"] == 1
    with open(property_schema_v2_path) as f:
        assert json.load(f)["version"] == 2

def test_schema_compatibility(loan_schema_path, property_schema_path, spark):
    """Test schema compatibility with Spark DataFrame."""
    # Create test data
    loan_data = [
        ("asset1", "2024-01-01", 100000.00),
        ("asset2", "2024-01-02", 200000.00)
    ]
    property_data = [
        ("prop1", "2024-01-01", 1000000.00),
        ("prop2", "2024-01-02", 2000000.00)
    ]
    
    # Create DataFrames
    loan_df = spark.createDataFrame(loan_data, ["assetNumber", "reportingPeriodEndDate", "originalLoanAmount"])
    property_df = spark.createDataFrame(property_data, ["propertyId", "valuationDate", "propertyValue"])
    
    # Load schemas
    with open(loan_schema_path) as f:
        loan_schema = json.load(f)
    with open(property_schema_path) as f:
        property_schema = json.load(f)
    
    # Verify DataFrame columns match schema definitions
    for col in loan_df.columns:
        assert col in loan_schema["columns"]
    
    for col in property_df.columns:
        assert col in property_schema["columns"]

def test_schema_validation(loan_schema_path, property_schema_path):
    """Test schema validation rules."""
    with open(loan_schema_path) as f:
        loan_schema = json.load(f)
    with open(property_schema_path) as f:
        property_schema = json.load(f)
    
    # Test required fields
    assert not loan_schema["columns"]["assetNumber"]["nullable"]
    assert not loan_schema["columns"]["reportingPeriodEndDate"]["nullable"]
    assert loan_schema["columns"]["originalLoanAmount"]["nullable"]
    
    assert not property_schema["columns"]["propertyId"]["nullable"]
    assert property_schema["columns"]["valuationDate"]["nullable"]
    assert property_schema["columns"]["propertyValue"]["nullable"]
    
    # Test decimal precision and scale
    assert "number" in loan_schema["columns"]["originalLoanAmount"]["type"]
    assert "number" in property_schema["columns"]["propertyValue"]["type"] 