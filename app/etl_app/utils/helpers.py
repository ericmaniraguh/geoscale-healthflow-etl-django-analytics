"""Utility functions for ETL operations"""

import uuid
import pandas as pd
from datetime import datetime
import re
import numpy as np
from typing import List, Optional


def format_timestamp(dt):
    """Format timestamp to YYYY-MM-DD HH:MM format"""
    return dt.strftime('%Y-%m-%d %H:%M')


def generate_unique_id():
    """Generate a unique ID for each record"""
    return str(uuid.uuid4())


def generate_dynamic_table_name(base_name: str, district: Optional[str] = None,
                               sector: Optional[str] = None, 
                               years_covered: Optional[List[int]] = None) -> str:
    """Generate static table names without years - consistent naming per district/sector"""
    
    # Clean and standardize the base name first
    clean_base_name = base_name.strip().lower()
    
    # Apply your requested abbreviations with new mappings
    name_mappings = {
        'health_center_raw_data': 'hc_raw',
        'health_center_lab_data': 'hc_raw',
        'hc_analytics_yearly_statistics': 'yearly_statist',  # Simplified as requested
        'hc_analytics_gender_pos_by_year': 'hc_data_gender_pos', 
        'hc_analytics_village_pos_by_year': 'hc_data_village_pos',
        'hc_analytics_total_summary': 'hc_data_sum',  # As shown in your examples
        'hc_analytics_monthly_positivity': 'hc_data_monthly_pos',
        'analytics_yearly_statistics': 'yearly_statist',
        'analytics_gender_pos_by_year': 'hc_data_gender_pos',
        'analytics_village_pos_by_year': 'hc_data_village_pos', 
        'analytics_total_summary': 'hc_data_sum',
        'analytics_monthly_positivity': 'hc_data_monthly_pos',
        'rwanda_boundaries_all': 'rwanda_boundaries',  # New mapping
        'hc_api_east_data': 'hc_api_east'  # New mapping for API data
    }
    
    # Get shortened base name
    short_base = name_mappings.get(clean_base_name, clean_base_name)
    
    # Apply additional abbreviations
    short_base = short_base.replace('statistics', 'statist')
    short_base = short_base.replace('health_center', 'hc')
    
    # Prevent unwanted automatic names
    if short_base.startswith('health_') and len(short_base) > 20:
        short_base = 'hc_raw'
    
    # Start building table name - NO YEARS INCLUDED
    parts = [short_base]
    
    # NOTE: Removed years section completely as requested
    
    # Clean and add district
    if district and district != 'all' and district.strip():
        clean_district = re.sub(r'[^a-zA-Z0-9]', '', district.lower().strip())
        if len(clean_district) > 0 and not clean_district.isdigit():
            parts.append(clean_district)
    
    # Clean and add sector
    if sector and sector != 'all' and sector.strip():
        clean_sector = re.sub(r'[^a-zA-Z0-9]', '', sector.lower().strip())
        if len(clean_sector) > 0 and not clean_sector.isdigit():
            parts.append(clean_sector)
    
    # Join all parts
    table_name = '_'.join(parts)
    
    # Handle PostgreSQL 63-character limit
    if len(table_name) > 63:
        # Strategy: Keep base name, shorten location names
        base_part = [short_base]
        remaining_length = 63 - len(short_base) - 2  # -2 for underscores
        
        locations = []
        if district and district != 'all' and district.strip():
            clean_district = re.sub(r'[^a-zA-Z0-9]', '', district.lower().strip())
            if clean_district and not clean_district.isdigit():
                locations.append(clean_district)
        
        if sector and sector != 'all' and sector.strip():
            clean_sector = re.sub(r'[^a-zA-Z0-9]', '', sector.lower().strip())
            if clean_sector and not clean_sector.isdigit():
                locations.append(clean_sector)
        
        # Fit locations within remaining space
        final_locations = []
        current_length = 0
        for loc in locations:
            if current_length + len(loc) + 1 <= remaining_length:
                final_locations.append(loc)
                current_length += len(loc) + 1
            else:
                # Try shortened version
                available_space = remaining_length - current_length - 1
                if available_space >= 3:
                    short_loc = loc[:available_space]
                    final_locations.append(short_loc)
                break
        
        table_name = '_'.join(base_part + final_locations)
    
    # Final cleanup
    table_name = re.sub(r'_+', '_', table_name)  # Remove multiple underscores
    table_name = table_name.strip('_')  # Remove leading/trailing underscores
    
    # Validate final name
    if len(table_name) == 0:
        table_name = short_base
    
    return table_name


def generate_simple_table_name(base_name: str, district: Optional[str] = None, sector: Optional[str] = None, years: Optional[List[int]] = None) -> str:
    """
    Simpler version - just concatenate years with underscores
    """
    parts = [base_name]
    
    if district:
        parts.append(district.lower().replace(' ', ''))
    if sector:
        parts.append(sector.lower().replace(' ', ''))
    if years:
        # Sort years and join with underscores
        sorted_years = sorted(set(years))  # Remove duplicates and sort
        parts.extend([str(y) for y in sorted_years])
    
    return '_'.join(parts).lower()


# Data cleaning functions
def clean_text(text):
    """Clean text fields"""
    if pd.isna(text) or not text:
        return ""
    return str(text).strip()


def clean_integer(value):
    """Clean integer fields"""
    try:
        if pd.isna(value) or value == "":
            return None
        return int(float(value))
    except:
        return None


def clean_gender(gender):
    """Clean and standardize gender field"""
    if pd.isna(gender) or not gender:
        return "Unknown"
    gender_str = str(gender).strip().upper()
    if gender_str in ['M', 'MALE', 'MAN']:
        return "Male"
    elif gender_str in ['F', 'FEMALE', 'WOMAN']:
        return "Female"
    return "Unknown"


def categorize_age(age):
    """Categorize age into groups"""
    if age is None:
        return "Unknown"
    if age < 5:
        return "Under 5"
    elif age < 15:
        return "5-14"
    elif age < 25:
        return "15-24"
    elif age < 45:
        return "25-44"
    elif age < 65:
        return "45-64"
    else:
        return "65+"


def clean_month(month_value):
    """Clean and convert month to integer"""
    if pd.isna(month_value) or not month_value:
        return None
    
    try:
        month_num = int(float(month_value))
        if 1 <= month_num <= 12:
            return month_num
    except:
        pass
    
    month_str = str(month_value).strip().lower()
    month_mapping = {
        'january': 1, 'jan': 1, 'february': 2, 'feb': 2,
        'march': 3, 'mar': 3, 'april': 4, 'apr': 4,
        'may': 5, 'june': 6, 'jun': 6, 'july': 7, 'jul': 7,
        'august': 8, 'aug': 8, 'september': 9, 'sep': 9,
        'october': 10, 'oct': 10, 'november': 11, 'nov': 11,
        'december': 12, 'dec': 12
    }
    return month_mapping.get(month_str)


def interpret_test_result(slide_status):
    """Interpret test result from slide status"""
    if pd.isna(slide_status) or not slide_status:
        return "Unknown"
    
    status_str = str(slide_status).strip().upper()
    positive_keywords = ['POSITIVE', 'POS', '+', 'P.FALCIPARUM', 'P.VIVAX', 'MALARIA']
    negative_keywords = ['NEGATIVE', 'NEG', '-', 'NO MALARIA', 'CLEAN']
    
    if any(keyword in status_str for keyword in positive_keywords):
        return "Positive"
    elif any(keyword in status_str for keyword in negative_keywords):
        return "Negative"
    return "Inconclusive"


def is_positive_case(slide_status):
    """Check if test result is positive"""
    return interpret_test_result(slide_status) == "Positive"


def sanitize_record(d: dict) -> dict:
    """Convert numpy types to native Python for safe DB insert."""
    clean = {}
    for k, v in d.items():
        if isinstance(v, (np.integer,)):   # np.int64, np.int32
            clean[k] = int(v)
        elif isinstance(v, (np.floating,)):  # np.float64, np.float32
            clean[k] = float(v)
        elif isinstance(v, (np.bool_)):   # np.bool_
            clean[k] = bool(v)
        elif isinstance(v, (np.str_, str)):  # numpy string
            clean[k] = str(v)
        else:
            clean[k] = v
    return clean
