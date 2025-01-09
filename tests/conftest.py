import os
import sys
import pytest
from pyspark.sql import SparkSession

# Get the absolute path of the project root directory
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

# Add project root to Python path if not already there
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Set Python environment variables for PySpark
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

@pytest.fixture(scope="session", autouse=True)
def setup_path():
    """Ensure project root is in Python path for all tests."""
    if project_root not in sys.path:
        sys.path.insert(0, project_root)
    yield

@pytest.fixture(scope="session")
def spark():
    """Create a Spark session for testing.
    
    This fixture is shared across all test files to avoid creating multiple Spark sessions.
    """
    spark = SparkSession.builder \
        .appName("unit-tests") \
        .master("local[1]") \
        .config("spark.sql.session.timeZone", "UTC") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .getOrCreate()
    yield spark
    spark.stop()

@pytest.fixture
def temp_dir(tmp_path):
    """Create a temporary directory for test files.
    
    This fixture provides a clean temporary directory for each test.
    """
    test_dir = tmp_path / "test_data"
    test_dir.mkdir()
    return test_dir

@pytest.fixture
def sample_data(spark):
    """Create sample data for testing.
    
    This fixture provides common test data used across multiple tests.
    """
    data = [
        ("asset1", "2024-01-01", 100.00, True, "file1.xml"),
        ("asset2", "2024-01-02", 200.00, False, "file1.xml"),
        ("asset3", "2024-01-03", 300.00, True, "file2.xml")
    ]
    return spark.createDataFrame(data, [
        "assetNumber",
        "reportingPeriodEndDate",
        "value",
        "flag",
        "source_file"
    ]) 