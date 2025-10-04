# etl_app/utils/constants.py
"""Constants and configurations for ETL operations"""

# Update modes
UPDATE_MODES = {
    'REPLACE': 'replace',
    'APPEND': 'append'
}

UPDATE_MODE_DESCRIPTIONS = {
    'replace': 'Drops existing tables and creates fresh data (default)',
    'append': 'Uses INSERT ... ON CONFLICT to update existing records or insert new ones'
}

# Default values
DEFAULT_TABLE_PREFIX = 'analytics'
DEFAULT_UPDATE_MODE = UPDATE_MODES['REPLACE']
DEFAULT_AGE = 30  # Default age for invalid/missing age values

# Age groups
AGE_GROUPS = {
    'UNDER_5': 'Under 5',
    '5_14': '5-14', 
    '15_24': '15-24',
    '25_44': '25-44',
    '45_64': '45-64',
    '65_PLUS': '65+'
}

# Gender mappings
GENDER_MAPPINGS = {
    'MALE_VARIANTS': ['M', 'MALE', 'MAN'],
    'FEMALE_VARIANTS': ['F', 'FEMALE', 'WOMAN'],
    'DEFAULT': 'Unknown'
}

# Test result keywords
TEST_RESULT_KEYWORDS = {
    'POSITIVE': ['POSITIVE', 'POS', '+', 'P.FALCIPARUM', 'P.VIVAX', 'MALARIA'],
    'NEGATIVE': ['NEGATIVE', 'NEG', '-', 'NO MALARIA', 'CLEAN']
}

# Month mappings
MONTH_NAMES = {
    1: 'January', 2: 'February', 3: 'March', 4: 'April',
    5: 'May', 6: 'June', 7: 'July', 8: 'August', 
    9: 'September', 10: 'October', 11: 'November', 12: 'December'
}

MONTH_ABBREVIATIONS = {
    'january': 1, 'jan': 1, 'february': 2, 'feb': 2,
    'march': 3, 'mar': 3, 'april': 4, 'apr': 4,
    'may': 5, 'june': 6, 'jun': 6, 'july': 7, 'jul': 7,
    'august': 8, 'aug': 8, 'september': 9, 'sep': 9,
    'october': 10, 'oct': 10, 'november': 11, 'nov': 11,
    'december': 12, 'dec': 12
}

# Database constraints
MAX_TABLE_NAME_LENGTH = 60  # PostgreSQL limit is 63, using 60 for safety
MAX_TABLE_PREFIX_LENGTH = 20
MAX_DISTRICT_SECTOR_LENGTH = 8  # For truncation in table names

# Year validation
MIN_VALID_YEAR = 2000
MAX_VALID_YEAR = 2100

# Age validation
MIN_VALID_AGE = 0
MAX_VALID_AGE = 120

# API response messages
SUCCESS_MESSAGES = {
    'DATA_PROCESSED': 'Successfully processed {count} records with dynamic table naming and unique IDs',
    'FILTERS_AVAILABLE': 'Available filters for Health Center Lab Data',
    'CONNECTION_SUCCESS': 'Database connections established successfully'
}

ERROR_MESSAGES = {
    'MONGO_CONNECTION_FAILED': 'Cannot connect to MongoDB',
    'POSTGRES_CONNECTION_FAILED': 'Cannot connect to PostgreSQL', 
    'NO_DATA_FOUND': 'No data found for the specified filters',
    'INVALID_JSON': 'Invalid JSON payload',
    'PROCESSING_ERROR': 'Error occurred during data processing'
}

# API examples
USAGE_EXAMPLES = {
    'process_all': '?years=all&calculate_analytics=true&save_to_db=true&update_mode=replace',
    'specific_year': '?years=2025&district=Kigali&sector=Nyarugenge&update_mode=append',
    'multiple_years': '?years=2025,2026&calculate_analytics=true&update_mode=replace',
    'custom_prefix': '?years=2025&table_prefix=malaria_analytics&update_mode=replace'
}

# Dynamic table naming examples
DYNAMIC_TABLE_EXAMPLES = {
    'yearly_statistics': 'analytics_yearly_statistics_[years]',
    'gender_pos': 'gender_pos_by_year_[district]_[sector]_[years]',
    'village_pos': 'village_pos_by_year_[district]_[sector]_[years]',
    'total_summary': 'total_summary_[district]_[sector]_[years]',
    'raw_data': 'raw_data_[district]_[sector]_[years]',
    'example': 'gender_pos_by_year_kigali_nyarugenge_2021_2022_2023'
}

# Features list
ETL_FEATURES = [
    'Auto-generated unique IDs for all records',
    'Smart update handling (replace/append modes)',
    'Formatted timestamps (YYYY-MM-DD HH:MM)',
    'Dynamic table naming based on filters',
    'Comprehensive analytics calculations',
    'PostgreSQL integration with indexing',
    'Modular service architecture'
]