# etl_app/services/data_transformer.py
"""Data cleaning and transformation service for health center lab data"""

import logging
import pandas as pd
from datetime import datetime
from typing import List, Dict, Any

from pendulum import timezone

from ..utils.helpers import (
    format_timestamp, 
    generate_unique_id,
    clean_text,
    clean_integer,
    clean_gender,
    categorize_age,
    clean_month,
    interpret_test_result,
    is_positive_case
)

logger = logging.getLogger(__name__)


class DataTransformer:
    """Service for data cleaning and transformation"""
    
    def __init__(self):
        pass


    def format_timestamp(dt):
        if isinstance(dt, str):
            """Format timestamp to ISO 8601 format"""
            dt = datetime.strptime(dt, "%Y-%m-%d %H:%M")
        return dt.astimezone(timezone.utc).isoformat()

    
    def clean_and_transform_data(self, raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Clean and transform raw data with unique IDs"""
        transformed_data = []
        
        for doc in raw_data:
            try:
                # Clean year
                year = None
                year_value = doc.get('Year')
                if year_value:
                    try:
                        year = int(float(year_value))
                    except:
                        pass
                
                # Clean month
                month = clean_month(doc.get('Month'))
                
                # Clean age
                age = clean_integer(doc.get('Age'))
                if age is None or age < 0 or age > 120:
                    age = 30  # Default age
                
                # Determine test result
                slide_status = str(doc.get('Slide Status', '')).strip()
                is_positive = is_positive_case(slide_status)
                test_result = interpret_test_result(slide_status)
                
                transformed_record = {
                    'unique_id': generate_unique_id(),  # Add unique ID
                    'year': year,
                    'month': month,
                    'district': clean_text(doc.get('District') or doc.get('_metadata_district')),
                    'sector': clean_text(doc.get('Sector') or doc.get('_metadata_sector')),
                    'health_center': clean_text(doc.get('Health Center') or doc.get('_metadata_health_center')),
                    'cell': clean_text(doc.get('Cell')),
                    'village': clean_text(doc.get('Village')),
                    'age': age,
                    'age_group': categorize_age(age),
                    'gender': clean_gender(doc.get('Gender')),
                    'slide_status': slide_status,
                    'test_result': test_result,
                    'is_positive': is_positive,
                    'case_origin': clean_text(doc.get('Case Origin')),
                    'province': clean_text(doc.get('Province')),
                    'created_at': format_timestamp(datetime.now()),  # Add formatted timestamp
                    'updated_at': format_timestamp(datetime.now())
                }
                
                transformed_data.append(transformed_record)
            except Exception as e:
                logger.warning(f"Error transforming record: {str(e)}")
                continue
        
        return transformed_data