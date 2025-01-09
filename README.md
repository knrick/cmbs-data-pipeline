# CMBS Data Pipeline

A comprehensive data pipeline for extracting, processing, and analyzing Commercial Mortgage-Backed Securities (CMBS) data from SEC EDGAR ABS-EE filings.

## Project Overview

This project creates an end-to-end data pipeline for CMBS loan-level data analysis, enabling investors and analysts to access structured, analysis-ready CMBS data from SEC filings.

### Current Status: Phase 1 Complete ✅

- ✅ SEC EDGAR scraping system for CMBS ABS-EE filings
- ✅ PySpark-based XML processing and validation
- ✅ Data quality profiling and metadata generation
- ✅ Comprehensive test coverage
- ✅ Configurable trust/issuer targeting

### Next Phase: Data Warehouse & Transformations 🚧

- 🔄 DBT models for data transformation
- 🔄 Data warehouse setup (Snowflake/PostgreSQL)
- 🔄 Data quality tests and documentation
- 🔄 Metric definitions and business logic

### Future Phases 📋

- 📊 Lightdash integration for visualization
- 🔄 Apache Airflow orchestration
- 🌐 FastAPI service layer
- 📈 Advanced analytics models

## Project Structure

```
SECPipeline/
├── spark_processors/     # PySpark processing modules
│   ├── xml_processor.py  # XML parsing and transformation
│   ├── data_profiler.py # Data quality analysis
│   └── schema_manager.py # Schema validation
├── utils/               # Utility modules
│   ├── cmbs_scraper.py # SEC EDGAR scraping
│   └── config.py       # Configuration management
├── tests/              # Test suite
│   └── ...            # Comprehensive tests
├── dbt/               # [Coming Soon] DBT models
├── airflow-docker/    # [Coming Soon] Airflow DAGs
└── api/               # [Coming Soon] FastAPI service
```

## Current Features

### Data Extraction
- Automated scraping of CMBS ABS-EE filings from SEC EDGAR
- Configurable company/trust targeting
- Rate-limited requests compliant with SEC guidelines
- Robust error handling and retry logic

### Data Processing
- Scalable XML processing using PySpark
- Schema validation and enforcement
- Data quality profiling and metadata generation
- Efficient storage format conversion

### Quality Assurance
- Comprehensive test suite
- Data validation rules
- Error logging and monitoring
- Performance optimization

## Upcoming Features

### Data Warehouse (Next Phase)
- Scalable data storage in Snowflake/PostgreSQL
- Incremental loading patterns
- Historical data tracking
- Efficient partitioning strategy

### DBT Implementation (Next Phase)
- Modular transformation models
- Data quality tests
- Documentation
- Metric definitions

### Future Enhancements
- Lightdash dashboards and visualizations
- Airflow-orchestrated workflows
- RESTful API access
- Advanced analytics capabilities

## Installation

1. Clone the repository:
```bash
git clone [repository-url]
```

2. Install core dependencies:
```bash
# For Spark processing
pip install -r spark_processors/requirements.txt

# For utilities
pip install -r utils/requirements.txt

# For testing
pip install -r tests/requirements.txt
```

## Usage

### Data Extraction
```python
from utils.cmbs_scraper import CMBSScraper

scraper = CMBSScraper()
scraper.get_search_table("Bank of America Merrill Lynch")
scraper.get_company_table()
```

### Data Processing
```python
from spark_processors.xml_processor import XMLProcessor

processor = XMLProcessor()
processor.process_directory("path/to/xml/files")
```

## Development Roadmap

1. **Current Phase (Complete)**
   - ✅ SEC EDGAR scraping
   - ✅ XML processing
   - ✅ Data validation
   - ✅ Testing

2. **Next Phase (Q1 2025)**
   - Data warehouse setup
   - DBT model development
   - Data quality framework
   - Documentation

3. **Future Phases (Q2-Q3 2025)**
   - Visualization layer
   - API development
   - Workflow orchestration
   - Advanced analytics

## Testing

Run the test suite:
```bash
pytest
```

For coverage report:
```bash
pytest --cov=spark_processors --cov=utils
```

## Contact

mail@alferov-dmitry.ru