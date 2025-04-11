"""
CMBS XML Processing Package

This package provides tools for processing CMBS XML files from SEC EDGAR,
including data validation, transformation, and storage in Parquet format.
"""

from .xml_processor import XMLProcessor
from .schema_manager import SchemaManager
from .data_profiler import DataProfiler
from .logging_utils import setup_logger
from .postgres import PostgresConnector, ParquetLoader

__version__ = "0.1.0"

__all__ = [
    "XMLProcessor",
    "SchemaManager",
    "DataProfiler",
    "setup_logger",
    "PostgresConnector",
    "ParquetLoader"
] 