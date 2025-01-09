# Spark Processors

This module contains PySpark-based processors for handling CMBS (Commercial Mortgage-Backed Securities) XML data from SEC EDGAR filings.

## Components

- `xml_processor.py`: Core processor for parsing and transforming XML data into structured Spark DataFrames
- `data_profiler.py`: Data quality and profiling tools for analyzing CMBS datasets
- `schema_manager.py`: Manages schema definitions and validation for CMBS data
- `logging_utils.py`: Utility functions for consistent logging across processors
- `property_schema.json`: Schema definition for property-level data
- `loan_schema.json`: Schema definition for loan-level data

## Installation

Install required dependencies:

```bash
pip install -r requirements.txt
```

## Dependencies

- pyspark>=3.4.0 - Apache Spark's Python API
- pandas>=2.0.0 - Data manipulation library
- numpy>=1.24.0 - Numerical computing library
- pytest>=7.4.0 - Testing framework
- python-dateutil>=2.8.2 - Date/time utilities

## Usage

### XML Processing

```python
from spark_processors.xml_processor import XMLProcessor

# Initialize processor
processor = XMLProcessor(output_dir="./processed_data")

# Process XML files in a directory
processor.process_directory("path/to/xml/files")
```

### Data Profiling

```python
from spark_processors.data_profiler import DataProfiler

# Initialize profiler
profiler = DataProfiler(profile_dir="./data_profiles")

# Profile a DataFrame
profile_results = profiler.get_column_stats(df, "column_name")
```

### Schema Management

```python
from spark_processors.schema_manager import SchemaManager

# Initialize schema manager
schema_manager = SchemaManager()

# Get Spark schema for loan data
loan_schema = schema_manager.get_spark_schema("loan")
```

## Testing

Run tests using pytest:

```bash
pytest tests/
``` 