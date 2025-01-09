# Test Suite

Comprehensive test suite for the CMBS data processing pipeline, covering both Spark processors and utility modules.

## Test Structure

- `conftest.py`: Common test fixtures and configuration
- `test_schema_manager.py`: Tests for schema validation and management
- `test_data_profiler.py`: Tests for data quality and profiling
- `test_base_schema.py`: Tests for base schema functionality

## Installation

Install test dependencies:

```bash
pip install -r requirements.txt
```

## Dependencies

- pytest>=7.4.0 - Testing framework
- pytest-cov>=4.1.0 - Coverage reporting
- pytest-xdist>=3.3.0 - Parallel test execution
- pytest-timeout>=2.1.0 - Test timeouts
- pyspark>=3.4.0 - Spark testing
- pandas>=2.0.0 - Data testing
- python-dateutil>=2.8.2 - Date handling

## Running Tests

### Run all tests:
```bash
pytest
```

### Run with coverage:
```bash
pytest --cov=spark_processors --cov=utils
```

### Run tests in parallel:
```bash
pytest -n auto
```

### Run specific test file:
```bash
pytest tests/test_schema_manager.py
```

### Run with verbose output:
```bash
pytest -v
```

## Test Categories

### Schema Tests
- Schema validation for all data types
- Invalid value handling
- Date format validation
- Numeric type validation
- String validation

### Data Profiling Tests
- Column statistics
- Data quality checks
- Value distribution analysis
- Anomaly detection

### Base Schema Tests
- Schema inheritance
- Schema versioning
- Schema compatibility

## Fixtures

The test suite provides several fixtures in `conftest.py`:

- `spark`: SparkSession for testing
- `temp_dir`: Temporary directory for test files
- `sample_data`: Common test datasets
- `setup_path`: Path configuration

## Writing New Tests

When adding new tests:

1. Use appropriate fixtures from `conftest.py`
2. Follow the existing test structure
3. Include both positive and negative test cases
4. Add proper assertions and error checks
5. Document test purpose and requirements

## Coverage Requirements

- Minimum coverage: 80%
- Branch coverage required
- Integration tests required for main workflows 