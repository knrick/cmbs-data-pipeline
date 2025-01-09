import os
import json

# Load CMBS companies from JSON
with open('companies.json', 'r') as f:
    COMPANY_NAMES = json.load(f)

# Data storage configuration
DATA_STORAGE = "./data"
LOCAL_ID_BATCH_SIZE = 500_000

# SEC API configuration
HEADERS = {
    "User-Agent": "Alferov Dmitry mail@alferov-dmitry.ru"
}

# Database configuration
DB_CONFIG = {
    "server": os.getenv("DB_SERVER", "localhost"),
    "database": os.getenv("DB_NAME", "cmbs_data"),
    "username": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD")
}

# Table names
DATA_SQL_NAMES = {
    "data": "CMBSFilingMonthly",
    "cat": "CMBSFilingCategories",
    "upload": "CMBSFilingUploadIds",
    "temp": "CMBSFilingIdsTemp"
}

SCRAPED_DATES_SQL_NAME = "CMBSScrapedDates"

# File naming
DATA_PICKLES = {
    "full": "full_data.pkl",
    "data": "data.pkl",
    "cat": "categories.pkl",
    "scraped_dates": "scraped_dates.pkl"
}

# CMBS-specific XML data columns
DATA_INCLUDE_COLS = [
    'id',
    'loanNumber',
    'propertyName',
    'propertyAddress',
    'propertyCity',
    'propertyState',
    'propertyZip',
    'propertyType',
    'netRentableSquareFeet',
    'numberofUnits',
    'yearBuilt',
    'valuationAmount',
    'valuationDate',
    'physicalOccupancy',
    'loanStatus',
    'originalLoanAmount',
    'currentLoanAmount',
    'interestRate',
    'paymentFrequency',
    'paymentType',
    'originationDate',
    'firstPaymentDate',
    'maturityDate',
    'debtServiceCoverageRatio',
    'mostRecentDebtServiceCoverageRatio',
    'mostRecentDebtServiceCoverageRatioDate',
    'Company',
    'Trust'
]

# Data type mappings
PANDAS_DATA_TYPES = {
    "id": "string",
    "loanNumber": "string",
    "propertyName": "string",
    "propertyAddress": "string",
    "propertyCity": "string",
    "propertyState": "string",
    "propertyZip": "string",
    "propertyType": "string",
    "netRentableSquareFeet": "float",
    "numberofUnits": "int",
    "yearBuilt": "int",
    "valuationAmount": "float",
    "valuationDate": "datetime",
    "physicalOccupancy": "float",
    "loanStatus": "string",
    "originalLoanAmount": "float",
    "currentLoanAmount": "float",
    "interestRate": "float",
    "paymentFrequency": "string",
    "paymentType": "string",
    "originationDate": "datetime",
    "firstPaymentDate": "datetime",
    "maturityDate": "datetime",
    "debtServiceCoverageRatio": "float",
    "mostRecentDebtServiceCoverageRatio": "float",
    "mostRecentDebtServiceCoverageRatioDate": "datetime",
    "Company": "string",
    "Trust": "string"
}

# Categorical and date columns
CAT_COLS = [
    'loanNumber',
    'propertyName',
    'propertyAddress',
    'propertyCity',
    'propertyState',
    'propertyZip',
    'propertyType',
    'paymentFrequency',
    'paymentType',
    'Company',
    'Trust'
]

DATE_COLS = [
    'valuationDate',
    'originationDate',
    'firstPaymentDate',
    'maturityDate',
    'mostRecentDebtServiceCoverageRatioDate'
]

# Processing configuration
CPU_COUNT = 8 