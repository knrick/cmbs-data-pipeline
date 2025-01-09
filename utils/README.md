# CMBS Utils

Utility modules for scraping and preprocessing CMBS (Commercial Mortgage-Backed Securities) data from SEC EDGAR filings.

## Components

- `cmbs_scraper.py`: Core scraper for downloading ABS-EE filings from SEC EDGAR
- `read_xml.py`: XML parsing utilities for CMBS filing data
- `config.py`: Configuration management and constants
- `__init__.py`: Package initialization and shared utilities

## Installation

Install required dependencies:

```bash
pip install -r requirements.txt
```

## Dependencies

- requests>=2.31.0 - HTTP library for API requests
- beautifulsoup4>=4.12.0 - Web scraping library
- lxml>=4.9.0 - XML parser
- pandas>=2.0.0 - Data manipulation library
- numpy>=1.24.0 - Numerical computing library
- python-dateutil>=2.8.2 - Date/time utilities

## Usage

### CMBS Scraping

```python
from utils.cmbs_scraper import CMBSScraper

# Initialize scraper
scraper = CMBSScraper()

# Get filings for a company
scraper.get_search_table("Bank of America Merrill Lynch")

# Download XML files
scraper.get_company_table()
```

### XML Reading

```python
from utils.read_xml import read_xml_file

# Read and parse XML file
data = read_xml_file("path/to/xml/file.xml")
```

### Configuration

```python
from utils.config import DATA_STORAGE, COMPANY_NAMES

# Access configuration constants
storage_path = DATA_STORAGE
companies = COMPANY_NAMES
```

## Features

- Automated scraping of CMBS ABS-EE filings from SEC EDGAR
- Rate-limited requests to comply with SEC guidelines
- Efficient XML parsing and preprocessing
- Configurable storage paths and company lists
- Robust error handling and logging

## Notes

- Respects SEC EDGAR rate limits (max 10 requests per second)
- Stores downloaded files in configurable directory structure
- Tracks scraped dates to avoid duplicate downloads
- Handles large XML files efficiently 